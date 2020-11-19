# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


from __future__ import division
from __future__ import print_function

import os
import numpy as np
import pandas as pd
import copy
from sklearn.metrics import roc_auc_score, mean_squared_error
import logging
from ...utils import unpack_archive_with_buffer, save_multiple_parts_file, create_save_path, drop_nan_by_y_index
from ...log import get_module_logger, TimeInspector

import torch
import torch.nn as nn
import torch.optim as optim

from ...model.base import Model
from ...data.dataset import DatasetH
from ...data.dataset.handler import DataHandlerLP


class GAT(Model):
    """GAT Model

    Parameters
    ----------
    input_dim : int
        input dimension
    output_dim : int
        output dimension
    layers : tuple
        layer sizes
    lr : float
        learning rate
    optimizer : str
        optimizer name
    GPU : str
        the GPU ID(s) used for training
    """

    def __init__(
        self,
        d_feat=6,
        hidden_size=64,
        num_layers=2,
        dropout=0.0,
        n_epochs=200,
        lr=0.001,
        metric="IC",
        batch_size=2000,
        early_stop=20,
        loss="mse",
        base_model="GRU",
        optimizer="adam",
        GPU="0",
        seed=0,
        **kwargs
    ):
        # Set logger.
        self.logger = get_module_logger("GAT")
        self.logger.info("GAT pytorch version...")

        # set hyper-parameters.
        self.d_feat = d_feat
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.dropout = dropout
        self.n_epochs = n_epochs
        self.lr = lr
        self.metric = metric
        self.batch_size = batch_size
        self.early_stop = early_stop
        self.optimizer = optimizer.lower()
        self.loss = loss
        self.base_model = base_model
        self.visible_GPU = GPU
        self.use_gpu = torch.cuda.is_available()
        self.seed = seed

        self.logger.info(
            "GAT parameters setting:"
            "\nd_feat : {}"
            "\nhidden_size : {}"
            "\nnum_layers : {}"
            "\ndropout : {}"
            "\nn_epochs : {}"
            "\nlr : {}"
            "\nmetric : {}"
            "\nbatch_size : {}"
            "\nearly_stop : {}"
            "\noptimizer : {}"
            "\nloss_type : {}"
            "\nbase_model : {}"
            "\nvisible_GPU : {}"
            "\nuse_GPU : {}"
            "\nseed : {}".format(
                d_feat,
                hidden_size,
                num_layers,
                dropout,
                n_epochs,
                lr,
                metric,
                batch_size,
                early_stop,
                optimizer.lower(),
                loss,
                base_model,
                GPU,
                self.use_gpu,
                seed,
            )
        )

        if loss not in {"mse", "binary"}:
            raise NotImplementedError("loss {} is not supported!".format(loss))
        self._scorer = mean_squared_error if loss == "mse" else roc_auc_score

        self.GAT_model = GATModel(
            d_feat=self.d_feat, hidden_size=self.hidden_size, num_layers=self.num_layers, dropout=self.dropout, base_model=self.base_model
        )
        if optimizer.lower() == "adam":
            self.train_optimizer = optim.Adam(self.GAT_model.parameters(), lr=self.lr)
        elif optimizer.lower() == "gd":
            self.train_optimizer = optim.SGD(self.GAT_model.parameters(), lr=self.lr)
        else:
            raise NotImplementedError("optimizer {} is not supported!".format(optimizer))

        self._fitted = False
        if self.use_gpu:
            self.GAT_model.cuda()
            # set the visible GPU
            if self.visible_GPU:
                os.environ["CUDA_VISIBLE_DEVICES"] = self.visible_GPU

    def mse(self, pred, label):
        loss = (pred - label) ** 2
        return torch.mean(loss)

    def loss_fn(self, pred, label):
        mask = ~torch.isnan(label)

        if self.loss == "mse":
            return self.mse(pred[mask], label[mask])

        raise ValueError("unknown loss `%s`" % self.loss)

    def metric_fn(self, pred, label):

        mask = torch.isfinite(label)
        if self.metric == "IC":
            return self.cal_ic(pred[mask], label[mask])

        if self.metric == "" or self.metric == "loss":  # use loss
            return -self.loss_fn(pred[mask], label[mask])

        raise ValueError("unknown metric `%s`" % self.metric)

    def cal_ic(self, pred, label):
        return torch.mean(pred * label)

    def train_epoch(self, x_train, y_train):

        x_train_values = x_train.values
        y_train_values = np.squeeze(y_train.values) * 100

        self.GAT_model.train()

        indices = np.arange(len(x_train_values))
        np.random.shuffle(indices)

        for i in range(len(indices))[:: self.batch_size]:

            if len(indices) - i < self.batch_size:
                break

            feature = torch.from_numpy(x_train_values[indices[i : i + self.batch_size]]).float()
            label = torch.from_numpy(y_train_values[indices[i : i + self.batch_size]]).float()

            if self.use_gpu:
                feature = feature.cuda()
                label = label.cuda()

            pred = self.GAT_model(feature)
            loss = self.loss_fn(pred, label)

            self.train_optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_value_(self.GAT_model.parameters(), 3.0)
            self.train_optimizer.step()

    def test_epoch(self, data_x, data_y):

        # prepare training data
        x_values = data_x.values
        y_values = np.squeeze(data_y.values)

        self.GAT_model.eval()

        scores = []
        losses = []

        indices = np.arange(len(x_values))
        np.random.shuffle(indices)

        for i in range(len(indices))[:: self.batch_size]:

            if len(indices) - i < self.batch_size:
                break

            feature = torch.from_numpy(x_values[indices[i : i + self.batch_size]]).float()
            label = torch.from_numpy(y_values[indices[i : i + self.batch_size]]).float()

            if self.use_gpu:
                feature = feature.cuda()
                label = label.cuda()

            pred = self.GAT_model(feature)
            loss = self.loss_fn(pred, label)
            losses.append(loss.item())

            score = self.metric_fn(pred, label)
            scores.append(score.item())

        return np.mean(losses), np.mean(scores)

    def fit(
        self,
        dataset: DatasetH,
        evals_result=dict(),
        verbose=True,
        save_path=None,
    ):

        df_train, df_valid, df_test = dataset.prepare(
            ["train", "valid", "test"], col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
        )

        x_train, y_train = df_train["feature"], df_train["label"]
        x_valid, y_valid = df_valid["feature"], df_valid["label"]

        if save_path == None:
            save_path = create_save_path(save_path)
        stop_steps = 0
        train_loss = 0
        best_score = -np.inf
        best_epoch = 0
        evals_result["train"] = []
        evals_result["valid"] = []

        # train
        self.logger.info("training...")
        self._fitted = True
        # return

        for step in range(self.n_epochs):
            self.logger.info("Epoch%d:", step)
            self.logger.info("training...")
            self.train_epoch(x_train, y_train)
            self.logger.info("evaluating...")
            train_loss, train_score = self.test_epoch(x_train, y_train)
            val_loss, val_score = self.test_epoch(x_valid, y_valid)
            self.logger.info("train %.6f, valid %.6f" % (train_score, val_score))
            evals_result["train"].append(train_score)
            evals_result["valid"].append(val_score)

            if val_score > best_score:
                best_score = val_score
                stop_steps = 0
                best_epoch = step
                best_param = copy.deepcopy(self.GAT_model.state_dict())
            else:
                stop_steps += 1
                if stop_steps >= self.early_stop:
                    self.logger.info("early stop")
                    break

        self.logger.info("best score: %.6lf @ %d" % (best_score, best_epoch))
        self.GAT_model.load_state_dict(best_param)
        torch.save(best_param, save_path)

        if self.use_gpu:
            torch.cuda.empty_cache()

    def predict(self, dataset):
        if not self._fitted:
            raise ValueError("model is not fitted yet!")

        x_test = dataset.prepare("test", col_set="feature")
        index = x_test.index
        self.GAT_model.eval()
        x_values = x_test.values
        sample_num = x_values.shape[0]
        preds = []

        for begin in range(sample_num)[:: self.batch_size]:

            if sample_num - begin < self.batch_size:
                end = sample_num
            else:
                end = begin + self.batch_size

            x_batch = torch.from_numpy(x_values[begin:end]).float()

            if self.use_gpu:
                x_batch = x_batch.cuda()

            with torch.no_grad():
                if self.use_gpu:
                    pred = self.GAT_model(x_batch).detach().cpu().numpy()
                else:
                    pred = self.GAT_model(x_batch).detach().numpy()

            preds.append(pred)

        return pd.Series(np.concatenate(preds), index=index)


class GATModel(nn.Module):
    
    def __init__(self, d_feat=6, hidden_size=64, num_layers=2, dropout=0.0, base_model='GRU'):
        super().__init__()

        if base_model == 'GRU':
            self.rnn = nn.GRU(
                input_size=d_feat,
                hidden_size=hidden_size,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout,
            )
        elif base_model == 'LSTM':
            self.rnn = nn.LSTM(
                input_size=d_feat,
                hidden_size=hidden_size,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout,
            )
        else:
            raise ValueError('unknown base model name `%s`'%base_model) 

        self.hidden_size = hidden_size
        self.bn1 = nn.BatchNorm1d(num_features=hidden_size, track_running_stats=False)
        self.fc = nn.Linear(hidden_size, hidden_size)
        self.bn2 = nn.BatchNorm1d(num_features=hidden_size, track_running_stats=False)
        self.fc_out = nn.Linear(hidden_size, 1)
        self.leaky_relu = nn.LeakyReLU()
        self.softmax = nn.Softmax(dim=1)

        self.d_feat = d_feat

    def cal_convariance(self, x, y): # the 2nd dimension of x and y are the same
        e_x = torch.mean(x, dim = 1).reshape(-1, 1)
        e_y = torch.mean(y, dim = 1).reshape(-1, 1)
        e_x_e_y = e_x.mm(torch.t(e_y))
        x_extend = x.reshape(x.shape[0], 1, x.shape[1]).repeat(1, y.shape[0], 1)
        y_extend = y.reshape(1, y.shape[0], y.shape[1]).repeat(x.shape[0], 1, 1)
        e_xy = torch.mean(x_extend*y_extend, dim = 2)
        return e_xy - e_x_e_y

    def forward(self, x):
        # x: [N, F*T]
        x = x.reshape(len(x), self.d_feat, -1) # [N, F, T]
        x = x.permute(0, 2, 1) # [N, T, F]
        out, _ = self.rnn(x)
        hidden = out[:, -1, :]
        hidden = self.bn1(hidden)

        gamma = self.cal_convariance(hidden, hidden)
        # gamma = hidden.mm(torch.t(hidden))
        # gamma = self.leaky_relu(gamma)
        # gamma = self.softmax(gamma)
        # gamma = gamma * (torch.ones(x.shape[0], x.shape[0]).to(device) - torch.diag(torch.ones(x.shape[0])).to(device))
        output = gamma.mm(hidden)
        output = self.fc(output)
        output = self.bn2(output)
        output = self.leaky_relu(output)
        return self.fc_out(output).squeeze()
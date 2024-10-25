import qlib
import pandas as pd
from qlib.data.dataset import DatasetH
from highfreq_handler import HighFreqHandler  # Assuming HighFreqHandler is defined in highfreq_handler.py

# Initialize Qlib
qlib.init(provider_uri='~/.qlib/custom_minute_data', region="us", freq='min')

# Define the dataset handler
handler = HighFreqHandler(
    start_time="2022-01-03 09:29:00",
    end_time="2022-12-30 16:38:00",
    fit_start_time="2022-01-03 09:29:00",
    fit_end_time="2022-11-30 16:03:00",
    instruments="all",
    infer_processors=[{"class": "HighFreqNorm", "module_path": "highfreq_processor"}],
)

# Create a dataset using DatasetH
dataset = DatasetH(handler=handler, segments={"train": ("2022-01-03 09:29:00", "2022-11-30 16:03:00"), "test": ("2022-12-01 09:30:00", "2022-12-30 16:38:00")})

# Load the training data to inspect
train_data = dataset.prepare("train")
print(train_data)

# Load the testing data to inspect
test_data = dataset.prepare("test")
print(test_data)

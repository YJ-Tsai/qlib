import pandas as pd
import os

def convert_polygon_to_qlib(symbols, input_dir, output_dir):
    # Ensure the output directory exists
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
    except PermissionError:
        print(f"Error: Permission denied when creating the output directory {output_dir}. Please check your permissions.")
        return

    for symbol in symbols:
        input_file = os.path.join(input_dir, f"{symbol}.csv")
        output_file = os.path.join(output_dir, f"{symbol}.csv")

        try:
            # Load the data from the input CSV file
            polygon_data = pd.read_csv(input_file)

            # Check for the 'Date' column and select it
            timestamp_column = 'Date'
            if timestamp_column not in polygon_data.columns:
                raise KeyError("The required timestamp column 'Date' is missing from the input data.")

            # Convert timestamp to date, handle different formats
            try:
                polygon_data['date'] = pd.to_datetime(polygon_data[timestamp_column]).dt.strftime('%Y-%m-%d')
            except Exception as e:
                raise ValueError(f"Error processing the date column '{timestamp_column}': {e}")

            # Rename columns to match Qlib's expected format
            polygon_data.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }, inplace=True)

            # Check if the required columns are present
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in polygon_data.columns]
            if missing_columns:
                raise KeyError(f"The following required columns are missing from the input data: {missing_columns}")

            # Add symbol column if not present
            polygon_data['symbol'] = symbol

            # Select only the required columns
            qlib_data = polygon_data[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

            # Set 'date' as the index
            qlib_data.set_index('date', inplace=True)

            # Save the reformatted data to the output CSV file
            qlib_data.to_csv(output_file)

            print(f"Successfully converted {symbol} data to Qlib format.")
        except FileNotFoundError:
            print(f"Error: File not found for symbol {symbol} at {input_file}")
        except KeyError as ke:
            print(f"Error processing symbol {symbol}: {ke}")
        except ValueError as ve:
            print(f"Error processing symbol {symbol}: {ve}")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")

# Example usage
symbols_list = ["MSFT", "GOOGL", "NVDA", "AMZN","AAPL"]  # Replace with your list of symbols
input_directory = "~/NaAn/data/index"  # Replace with your input directory path
output_directory = "/home/yungt/qlib_data"  # Replace with your output directory path

convert_polygon_to_qlib(symbols_list, input_directory, output_directory)

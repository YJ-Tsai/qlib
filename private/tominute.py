import pandas as pd
import os
import datetime

def convert_polygon_to_qlib_minute(symbols, input_dir, output_dir, start_date, end_date):
    # Ensure the output directory exists
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
    except PermissionError:
        print(f"Error: Permission denied when creating the output directory {output_dir}. Please check your permissions.")
        return

    # Generate list of all dates between start_date and end_date
    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

    for symbol in symbols:
        symbol_input_dir = os.path.join(input_dir, symbol)
        output_file = os.path.join(output_dir, f"{symbol}.csv")

        all_data = []
        for date in date_range:
            input_file = os.path.join(symbol_input_dir, f"{date}.csv")
            try:
                # Load the data from the input CSV file for each date
                polygon_data = pd.read_csv(input_file)

                # Check for the 'Date' column and select it
                timestamp_column = 'Date'
                if timestamp_column not in polygon_data.columns:
                    raise KeyError("The required timestamp column 'Date' is missing from the input data.")

                # Convert timestamp to date-time, handle different formats
                try:
                    polygon_data['date'] = pd.to_datetime(polygon_data[timestamp_column]).dt.strftime('%Y-%m-%d %H:%M:%S')
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
                all_data.append(qlib_data)

            except FileNotFoundError:
                print(f"Error: File not found for symbol {symbol} on date {date} at {input_file}")
            except KeyError as ke:
                print(f"Error processing symbol {symbol} on date {date}: {ke}")
            except ValueError as ve:
                print(f"Error processing symbol {symbol} on date {date}: {ve}")
            except Exception as e:
                print(f"Error processing symbol {symbol} on date {date}: {e}")

        # Concatenate all the data for the symbol
        if all_data:
            combined_data = pd.concat(all_data)
            # Set 'date' as the index
            combined_data.set_index('date', inplace=True)
            # Save the reformatted data to the output CSV file
            combined_data.to_csv(output_file)
            print(f"Successfully converted {symbol} minute data to Qlib format.")

# Example usage
symbols_list = ["A"]  # Replace with your list of symbols
input_directory = "~/datastore/minute"  # Replace with your input directory path
output_directory = "/mnt/data/qlib_data/minute"  # Replace with your output directory path
start_day = "2022-01-01"  # Replace with your start date
end_day = "2022-12-31"  # Replace with your end date

convert_polygon_to_qlib_minute(symbols_list, input_directory, output_directory, start_day, end_day)

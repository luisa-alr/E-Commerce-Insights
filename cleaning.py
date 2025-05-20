import pandas as pd
import numpy as np
import os

input_file = '2019-Oct.csv'  
output_folder = './split_data'  
os.makedirs(output_folder, exist_ok=True)

def price_group(price, low_thresh, high_thresh):
    if price <= low_thresh:
        return 'Low'
    elif price <= high_thresh:
        return 'Medium'
    else:
        return 'High'  

def time_of_day(hour):
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 18:
        return 'Afternoon'
    else:
        return 'Night'

# Estimate global price thresholds
print("Estimating global price thresholds...")

price_values = pd.read_csv(input_file, usecols=['price'], chunksize=500_000)

all_prices = []
for chunk in price_values:
    all_prices.extend(chunk['price'].dropna().tolist())

all_prices = pd.Series(all_prices)
low_thresh, high_thresh = all_prices.quantile([0.33, 0.66]).values

print(f"Global price thresholds:")
print(f"- Low <= {low_thresh:.2f}")
print(f"- Medium <= {high_thresh:.2f}")
print(f"- High > {high_thresh:.2f}")

print("Processing...")

reader = pd.read_csv(input_file, chunksize=500_000)
for i, chunk in enumerate(reader):
    print(f"\nProcessing chunk {i+1}...")

    # Remove any rows with missing values (any column)
    chunk.dropna(inplace=True)

    # Convert event_time to datetime
    chunk['event_time'] = pd.to_datetime(chunk['event_time'], errors='coerce')
    chunk.dropna(subset=['event_time'], inplace=True)

    # Create price_chunk column (using GLOBAL thresholds)
    chunk['price_chunk'] = chunk['price'].apply(lambda x: price_group(x, low_thresh, high_thresh))
    
    # Create main_category column
    chunk['main_category'] = chunk['category_code'].apply(lambda x: str(x).split('.')[0] if pd.notnull(x) else None)

    # Create time_of_day column
    chunk['event_hour'] = chunk['event_time'].dt.hour
    chunk['time_of_day'] = chunk['event_hour'].apply(time_of_day)

    # Save the processed chunk
    output_file = os.path.join(output_folder, f'chunk_{i+1}.csv')
    chunk.to_csv(output_file, index=False)
    print(f"Saved {output_file} with {len(chunk)} rows.")

    # STOP after 38 chunks (controlled amount of data)
    if i+1 == 150:
        print("\nStopping after 150 chunks!")
        break

print("\nCleaning complete!")

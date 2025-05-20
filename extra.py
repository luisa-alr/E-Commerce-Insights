import pandas as pd
import os

input_folder = './split_data'
chunk_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])

print("Loading all chunks...")
dfs = [pd.read_csv(os.path.join(input_folder, f), low_memory=False) for f in chunk_files]
df = pd.concat(dfs, ignore_index=True)
print(f"Loaded {len(df):,} total events.")

total_events = len(df)
unique_users = df['user_id'].nunique()
unique_sessions = df['user_session'].nunique()
event_dist = df['event_type'].value_counts(normalize=True).round(4) * 100
event_dist = event_dist.to_dict()

# brand
num_unique_brands = df['brand'].nunique()
brand_category_map = df.dropna(subset=['brand', 'main_category'])[['brand', 'main_category']].drop_duplicates()

# sessions
df['event_time'] = pd.to_datetime(df['event_time'], utc=True)

session_stats = df.groupby('user_session').agg({
    'event_time': [min, max],
    'product_id': pd.Series.nunique,
    'brand': pd.Series.nunique
}).reset_index()

session_stats.columns = ['user_session', 'start_time', 'end_time', 'unique_products', 'unique_brands']
session_stats['session_duration_sec'] = (session_stats['end_time'] - session_stats['start_time']).dt.total_seconds()

# summary
print("\n--- Dataset Summary ---")
print(f"Time Period: October 2019")
print(f"Total Events: {total_events:,}")
print(f"Unique Users: {unique_users:,}")
print(f"Unique Sessions: {unique_sessions:,}")
print(f"Unique Brands: {num_unique_brands:,}")
print("\nEvent Type Distribution:")
for k, v in event_dist.items():
    print(f" - {k}: {v:.2f}%")

# save breakdowns
brand_category_map.to_csv("brand_maincategory_map.csv", index=False)
print("\nSaved session_analysis.csv and brand_maincategory_map.csv")
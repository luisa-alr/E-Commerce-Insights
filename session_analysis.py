import pandas as pd
import os

input_folder = './split_data'
output_path = './session_analysis_with_patterns.csv'

chunk_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])
print("Loading all chunks...")
dfs = [pd.read_csv(os.path.join(input_folder, f), low_memory=False) for f in chunk_files]
df = pd.concat(dfs, ignore_index=True)
print(f"Loaded {len(df):,} total events.")

df['event_time'] = pd.to_datetime(df['event_time'], utc=True)

print("Generating session features...")

# Group by session to extract event sequence
session_patterns = df.sort_values(['user_session', 'event_time']).groupby('user_session')['event_type'].apply(list).reset_index()
session_patterns['event_pattern'] = session_patterns['event_type'].apply(lambda x: ' ➔ '.join(x))
session_patterns.drop(columns='event_type', inplace=True)

# Get session start/end times and unique counts
session_stats = df.groupby('user_session').agg({
    'event_time': [min, max],
    'product_id': pd.Series.nunique,
    'brand': pd.Series.nunique
}).reset_index()

session_stats.columns = ['user_session', 'start_time', 'end_time', 'unique_products', 'unique_brands']
session_stats['session_duration_sec'] = (session_stats['end_time'] - session_stats['start_time']).dt.total_seconds()

final_df = pd.merge(session_stats, session_patterns, on='user_session')

# save
final_df.to_csv(output_path, index=False)
print(f"Saved session summary with patterns to {output_path}")

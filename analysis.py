import pandas as pd
import numpy as np
import os
from collections import Counter
from prefixspan import PrefixSpan

input_folder = './split_data'
output_folder = './analysis_results'
os.makedirs(output_folder, exist_ok=True)

chunk_files = [f'chunk_{i}.csv' for i in range(1, 86)]

# Parameters
TOP_K = 10
MIN_LEN_GENERAL = 2
MIN_LEN_INTERACTION = 2
MIN_LEN_INTERACTION_STRONG = 3

def contains_subsequence(session, pattern):
    pattern_idx = 0
    for event in session:
        if event == pattern[pattern_idx]:
            pattern_idx += 1
            if pattern_idx == len(pattern):
                return True
    return False

def session_has_interaction(session):
    return any(x in session for x in ['cart', 'purchase', 'remove_from_cart'])

def extract_patterns(sequences, min_len=2, interaction_only=False):
    ps = PrefixSpan(sequences)
    patterns = ps.topk(k=500, closed=False)
    filtered_patterns = []
    for support, seq in patterns:
        if len(seq) >= min_len:
            if not interaction_only or any(x in seq for x in ['cart', 'purchase', 'remove_from_cart']):
                filtered_patterns.append((support, seq))
    return filtered_patterns[:TOP_K]

def count_full_sessions(session_sequences):
    return Counter([tuple(seq) for seq in session_sequences])

# Load Data
print("Loading all chunks...")
dfs = []
for file in chunk_files:
    df = pd.read_csv(os.path.join(input_folder, file), low_memory=False)
    dfs.append(df)

data = pd.concat(dfs, ignore_index=True)
print(f"Loaded {len(data)} rows.")

# Clean Data
print("Cleaning data...")
data['event_time'] = pd.to_datetime(data['event_time'], utc=True)
data['event_hour'] = data['event_time'].dt.hour
data['time_of_day'] = data['event_hour'].apply(lambda h:
    'Morning' if 5 <= h < 12 else
    'Afternoon' if 12 <= h < 17 else
    'Evening' if 17 <= h < 22 else
    'Night'
)

data.dropna(subset=['event_type', 'product_id', 'user_session', 'price', 'price_chunk', 'main_category'], inplace=True)

print("Building sessions...")
session_sequences = data.sort_values(['user_session', 'event_time']).groupby('user_session')['event_type'].apply(list)
session_avg_price = data.groupby('user_session')['price'].mean()
session_time_of_day = data.groupby('user_session')['time_of_day'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
session_price_chunk = data.groupby('user_session')['price_chunk'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
session_main_category = data.groupby('user_session')['main_category'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
session_duration_sec = data.groupby('user_session')['event_time'].agg(lambda x: (x.max() - x.min()).total_seconds())

session_df = pd.DataFrame({
    'session_sequence': session_sequences,
    'avg_price': session_avg_price,
    'time_of_day': session_time_of_day,
    'price_chunk': session_price_chunk,
    'main_category': session_main_category,
    'duration_sec': session_duration_sec
}).reset_index()

session_df = session_df[session_df['session_sequence'].apply(session_has_interaction)]

# Run analysis
def analyze_section(group_label, group_sessions, section_name):
    print(f"Analyzing section {section_name}: {group_label}...")
    results = []

    # FULL session matches
    full_counts = count_full_sessions(group_sessions['session_sequence'])
    top_full_sessions = full_counts.most_common(TOP_K)

    for pattern, count in top_full_sessions:
        matched = group_sessions[group_sessions['session_sequence'].apply(lambda s: s == list(pattern))]
        results.append({
            'section': section_name,
            'subgroup': group_label,
            'type': 'full_session',
            'pattern': pattern,
            'occurrences': count,
            'avg_price': matched['avg_price'].mean(),
            'common_time_of_day': matched['time_of_day'].mode().iloc[0] if not matched['time_of_day'].empty else None,
            'avg_duration_sec': matched['duration_sec'].mean()
        })

    # PrefixSpan sessions
    for label, min_len, interaction_only in [
        ('prefixspan_general', MIN_LEN_GENERAL, False),
        ('prefixspan_interaction_gt2', MIN_LEN_INTERACTION, True),
        ('prefixspan_interaction_gt3', MIN_LEN_INTERACTION_STRONG, True)
    ]:
        patterns = extract_patterns(group_sessions['session_sequence'].tolist(), min_len=min_len, interaction_only=interaction_only)
        for support, seq in patterns:
            matched = group_sessions[group_sessions['session_sequence'].apply(lambda s: contains_subsequence(s, seq))]
            results.append({
                'section': section_name,
                'subgroup': group_label,
                'type': label,
                'pattern': seq,
                'occurrences': support,
                'avg_price': matched['avg_price'].mean(),
                'common_time_of_day': matched['time_of_day'].mode().iloc[0] if not matched['time_of_day'].empty else None,
                'avg_duration_sec': matched['duration_sec'].mean()
            })

    return results

final_results = []

for price in session_df['price_chunk'].dropna().unique():
    subset = session_df[session_df['price_chunk'] == price]
    final_results.extend(analyze_section(price, subset, 'price_chunk'))

for category in session_df['main_category'].dropna().unique():
    subset = session_df[session_df['main_category'] == category]
    final_results.extend(analyze_section(category, subset, 'main_category'))

for tod in session_df['time_of_day'].dropna().unique():
    subset = session_df[session_df['time_of_day'] == tod]
    final_results.extend(analyze_section(tod, subset, 'time_of_day'))

results_df = pd.DataFrame(final_results)
results_df.to_csv(os.path.join(output_folder, 'deep_dive_patterns.csv'), index=False)
print(f"\nAll deep dive patterns saved!")

# Cart metrics
def calculate_cart_metrics(group_label, group_sessions, section_name):
    cart_sessions = group_sessions[group_sessions['session_sequence'].apply(lambda seq: 'cart' in seq)]

    if len(cart_sessions) == 0:
        return {
            'section': section_name,
            'subgroup': group_label,
            'cart_sessions': 0,
            'cart_abandonment_rate': None,
            'cart_to_remove_rate': None,
            'cart_to_purchase_rate': None,
            'most_common_time_purchase': None,
            'most_common_time_remove': None
        }

    abandon = 0
    cart_to_remove = 0
    cart_to_purchase = 0
    purchase_times = []
    remove_times = []

    for idx, row in cart_sessions.iterrows():
        seq = row['session_sequence']
        if 'purchase' in seq and seq.index('purchase') > seq.index('cart'):
            cart_to_purchase += 1
            purchase_times.append(row['time_of_day'])
        elif 'remove_from_cart' in seq and seq.index('remove_from_cart') > seq.index('cart'):
            cart_to_remove += 1
            remove_times.append(row['time_of_day'])
        else:
            abandon += 1

    total = len(cart_sessions)
    return {
        'section': section_name,
        'subgroup': group_label,
        'cart_sessions': total,
        'cart_abandonment_rate': (abandon / total) * 100,
        'cart_to_remove_rate': (cart_to_remove / total) * 100,
        'cart_to_purchase_rate': (cart_to_purchase / total) * 100,
        'most_common_time_purchase': pd.Series(purchase_times).mode().iloc[0] if purchase_times else None,
        'most_common_time_remove': pd.Series(remove_times).mode().iloc[0] if remove_times else None
    }

funnel_results = []

for price in session_df['price_chunk'].dropna().unique():
    subset = session_df[session_df['price_chunk'] == price]
    funnel_results.append(calculate_cart_metrics(price, subset, 'price_chunk'))

for category in session_df['main_category'].dropna().unique():
    subset = session_df[session_df['main_category'] == category]
    funnel_results.append(calculate_cart_metrics(category, subset, 'main_category'))

for tod in session_df['time_of_day'].dropna().unique():
    subset = session_df[session_df['time_of_day'] == tod]
    funnel_results.append(calculate_cart_metrics(tod, subset, 'time_of_day'))

funnel_df = pd.DataFrame(funnel_results)
funnel_df.to_csv(os.path.join(output_folder, 'cart_behavior_metrics.csv'), index=False)
print(f"\nBehavioral Metrics saved!")
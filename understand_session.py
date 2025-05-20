import pandas as pd

file_path = './session_analysis_with_patterns.csv'
session_df = pd.read_csv(file_path)

# Ensure datetime columns
session_df['start_time'] = pd.to_datetime(session_df['start_time'], utc=True)
session_df['end_time'] = pd.to_datetime(session_df['end_time'], utc=True)

# --- OVERALL STATS ---
overall_stats = {
    'total_sessions': len(session_df),
    'avg_session_duration_sec': session_df['session_duration_sec'].mean(),
    'median_session_duration_sec': session_df['session_duration_sec'].median(),
    'avg_unique_products': session_df['unique_products'].mean(),
    'avg_unique_brands': session_df['unique_brands'].mean(),
}

print("\n--- Overall Session Statistics ---")
for k, v in overall_stats.items():
    print(f"{k.replace('_', ' ').title()}: {v:.2f}")

# --- TOP PATTERN COUNTS ---
pattern_counts = session_df['event_pattern'].value_counts().reset_index()
pattern_counts.columns = ['event_pattern', 'num_sessions']

print("\n--- Top 10 Most Common Event Patterns ---")
print(pattern_counts.head(10).to_string(index=False))

# --- TOP PATTERN STATS ---
top_patterns = pattern_counts['event_pattern'].head(10).tolist()
pattern_analysis = []

for pattern in top_patterns:
    subset = session_df[session_df['event_pattern'] == pattern]
    pattern_analysis.append({
        'event_pattern': pattern,
        'num_sessions': len(subset),
        'avg_session_duration_sec': subset['session_duration_sec'].mean(),
        'median_session_duration_sec': subset['session_duration_sec'].median(),
        'avg_unique_products': subset['unique_products'].mean(),
        'avg_unique_brands': subset['unique_brands'].mean()
    })

pattern_stats_df = pd.DataFrame(pattern_analysis)

print("\n--- Detailed Stats for Top 10 Patterns ---")
print(pattern_stats_df)

# --- BROWSING VS INTERACTION ---
def is_browsing_only(pattern):
    actions = ['cart', 'remove', 'purchase']
    return not any(action in pattern for action in actions)

session_df['is_browsing_only'] = session_df['event_pattern'].apply(is_browsing_only)

browsing_sessions = session_df[session_df['is_browsing_only']]
interaction_sessions = session_df[~session_df['is_browsing_only']]

# Browsing stats
browsing_stats = {
    'total_browsing_sessions': len(browsing_sessions),
    'avg_browsing_duration_sec': browsing_sessions['session_duration_sec'].mean(),
    'avg_unique_products_browsing': browsing_sessions['unique_products'].mean(),
    'avg_unique_brands_browsing': browsing_sessions['unique_brands'].mean()
}

# Interaction stats
interaction_stats = {
    'total_interaction_sessions': len(interaction_sessions),
    'avg_interaction_duration_sec': interaction_sessions['session_duration_sec'].mean(),
    'avg_unique_products_interaction': interaction_sessions['unique_products'].mean(),
    'avg_unique_brands_interaction': interaction_sessions['unique_brands'].mean()
}

print("\n--- Browsing Only Sessions ---")
for k, v in browsing_stats.items():
    print(f"{k.replace('_', ' ').title()}: {v:.2f}")

print("\n--- Sessions with Interactions (Cart/Remove/Purchase) ---")
for k, v in interaction_stats.items():
    print(f"{k.replace('_', ' ').title()}: {v:.2f}")

# --- INTERACTION PATTERNS ---
interaction_patterns_df = interaction_sessions['event_pattern'].value_counts().reset_index()
interaction_patterns_df.columns = ['event_pattern', 'num_sessions']

print("\n--- Top 10 Most Common Event Patterns (with Interactions) ---")
print(interaction_patterns_df.head(10).to_string(index=False))

print("\n⏱️ Avg Duration for Top Interaction Patterns:")
for pattern in interaction_patterns_df['event_pattern'].head(10):
    subset = interaction_sessions[interaction_sessions['event_pattern'] == pattern]
    print(f"Pattern: {pattern}")
    print(f"  Sessions: {len(subset)}")
    print(f"  Avg Duration: {subset['session_duration_sec'].mean():.2f} sec")
    print(f"  Avg Unique Products: {subset['unique_products'].mean():.2f}")
    print(f"  Avg Unique Brands: {subset['unique_brands'].mean():.2f}")
    print()

# --- SAVE OUTPUTS ---
pattern_counts.to_csv('pattern_counts.csv', index=False)
pattern_stats_df.to_csv('pattern_stats_top10.csv', index=False)
browsing_sessions.to_csv('browsing_only_sessions.csv', index=False)
interaction_sessions.to_csv('interaction_sessions.csv', index=False)
interaction_patterns_df.to_csv('interaction_pattern_counts.csv', index=False)

print("\n✅ Analysis complete!")
print("Files saved: pattern_counts.csv, pattern_stats_top10.csv, browsing_only_sessions.csv, interaction_sessions.csv, interaction_pattern_counts.csv")


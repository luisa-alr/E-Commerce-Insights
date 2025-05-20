import pandas as pd

price_df = pd.read_csv('./analysis_results/price_chunk.csv')
category_df = pd.read_csv('./analysis_results/main_category.csv')
time_df = pd.read_csv('./analysis_results/time_of_day.csv')

TOP_N = 5  # Top N patterns to keep per group

# Function for analysis
def analyze_section(df, section_name):
    print(f"\nAnalyzing section: {section_name}")

    results = []

    total_patterns = len(df)  # Overall (mostly informational)

    for subgroup, group in df.groupby('subgroup'):
        print(f"Subgroup: {subgroup} ({len(group)} patterns)")

        subgroup_total = len(group)

        # Full session top patterns
        full_patterns = group[group['type'] == 'full_session'].sort_values('occurrences', ascending=False).head(TOP_N)

        # PrefixSpan (interaction sequences, length ≥3)
        prefix_patterns = group[group['type'] == 'prefixspan_interaction_gt3'].sort_values('occurrences', ascending=False).head(TOP_N)

        # Summarize full session patterns
        for rank, (_, row) in enumerate(full_patterns.iterrows(), start=1):
            results.append({
                'section': section_name,
                'subgroup': subgroup,
                'pattern_type': 'full_session',
                'pattern': row['pattern'],
                'occurrences': row['occurrences'],
                'occurrences_pct': (row['occurrences'] / subgroup_total) * 100 if subgroup_total else None,
                'avg_price': row['avg_price'],
                'avg_duration_sec': row.get('avg_duration_sec', None),  # Full session has this
                'common_time_of_day': row['common_time_of_day'],
                'rank': rank
            })

        # Summarize prefixspan patterns
        for rank, (_, row) in enumerate(prefix_patterns.iterrows(), start=1):
            results.append({
                'section': section_name,
                'subgroup': subgroup,
                'pattern_type': 'prefixspan_interaction_gt3',
                'pattern': row['pattern'],
                'occurrences': row['occurrences'],
                'occurrences_pct': (row['occurrences'] / subgroup_total) * 100 if subgroup_total else None,
                'avg_price': row['avg_price'],
                'avg_duration_sec': None,  # PrefixSpan patterns don't have full session durations
                'common_time_of_day': row['common_time_of_day'],
                'rank': rank
            })

    return pd.DataFrame(results)

# Run Analysis
print("\n Starting analysis...")
price_summary = analyze_section(price_df, 'price_chunk')
category_summary = analyze_section(category_df, 'main_category')
time_summary = analyze_section(time_df, 'time_of_day')

# save output
output_folder = './analysis_results'

price_summary.to_csv(f'{output_folder}/summary_price_chunk.csv', index=False)
category_summary.to_csv(f'{output_folder}/summary_main_category.csv', index=False)
time_summary.to_csv(f'{output_folder}/summary_time_of_day.csv', index=False)

print("\n Summaries saved successfully in 'analysis_results' folder!")


# E-Commerce Behavioral Patterns and Consumer Insights

**Author:** Luisa Rosa   
**Capstone Project**

## Overview

This project analyzes user behavior in an e-commerce platform based on session events (e.g., view, cart, purchase). It extracts meaningful patterns, performs behavioral segmentation, and visualizes trends to uncover insights for user behavior modeling.

---

## Project Workflow

The project should be run in **five key stages**, in this order:

### 1. **Data Cleaning**

**Script:** `cleaning.py`  
Cleans the raw data file (`2019-Oct.csv`) and splits it into smaller chunks with enhanced features:
- Session-level datetime parsing
- `price_chunk` labels (Low/Medium/High)
- Time-of-day classification (Morning/Afternoon/Night)
- Category simplification via `main_category`

**Output:**  
Cleaned files saved in `./split_data/` directory.

---

### 2. **Session Pattern Analysis**

**Script:** `analysis.py`  
- Builds user sessions from the cleaned chunks
- Extracts interaction patterns using full session matching and PrefixSpan
- Calculates session-level metrics and cart conversion behaviors

**Output:**  
- `deep_dive_patterns.csv`  
- `cart_behavior_metrics.csv`  
(saved in `./analysis_results/`)

---

### 3. **Extra Dataset Metrics**

**Script:** `extra.py`  
Provides a general snapshot of the dataset:
- Event distribution
- Unique users/sessions/brands
- Mapping between brands and categories

**Output:**  
- `brand_maincategory_map.csv`

---

### 4. **Visualization & Plotting**

**Script:** `plotting.py`  
Generates visualizations from the pattern summary files:
- Average price per subgroup
- Occurrence heatmaps
- Time-of-day pattern trends

**Output:**  
Multiple `.png` plots saved in `./analysis_results/`

---

### 5. **Session Analysis Summary**

#### A. Session Pattern Summarization

**Script:** `session_analysis.py`  
Generates summarized session stats + common event patterns.

**Output:**  
- `session_analysis_with_patterns.csv`

#### B. Insight Extraction

**Script:** `understand_session.py`  
Analyzes:
- Top session sequences
- Session durations and product diversity
- Browsing-only vs. interactive behavior

**Output:**  
Multiple CSV summaries including:
- `pattern_counts.csv`
- `interaction_sessions.csv`
- `pattern_stats_top10.csv`  
...and more

#### C. Grouped Pattern Summarization

**Script:** `understand_patterns.py`  
Combines and summarizes top patterns per subgroup (price, category, time-of-day).

**Output:**  
- `summary_price_chunk.csv`
- `summary_main_category.csv`
- `summary_time_of_day.csv`

---

## Requirements

- Python 3.7+
- pandas
- numpy
- matplotlib
- seaborn
- [prefixspan](https://pypi.org/project/prefixspan/)

Install dependencies:

```bash
pip install pandas numpy matplotlib seaborn prefixspan
```

---

## Notes

- Ensure the raw file `2019-Oct.csv` is available in the root directory.
- The cleaning script is capped to process 150 chunks (~7.5 million rows) for speed.
- Modify thresholds or categories directly in the scripts if adapting for other datasets.

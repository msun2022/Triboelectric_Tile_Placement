# Data Cleaning and mean_counts.csv Generation

## Overview

This document describes the data cleaning pipeline and the process for generating `mean_counts.csv` from the raw `bike_ped_counts.csv` file. The pipeline aggregates pedestrian, jogger, and child traffic counts by location and projects them into a unified weighted metric.

## Data Cleaning Process

### Step 1: Reading and Initial Aggregation

The raw data is read from `bike_ped_counts.csv` using Julia's CSV.jl package:

```julia
raw_df = CSV.read("bike_ped_counts.csv", DataFrame; stringtype=String)
```

The data is then aggregated by grouping on three key dimensions:
- `bp_loc_id`: Location identifier
- `count_type`: Type of traffic count (B=bicycle, P=pedestrian, J=jogger, C=child, etc.)
- `count_date`: Date of the count

For each unique combination of these three fields, the total counts (`cnt_total`) are summed:

```julia
total_counts = combine(
    groupby(raw_df, [:bp_loc_id, :count_type, :count_date]),
    :cnt_total => sum => :loc_total
)
```

This aggregation step combines multiple count records that may exist for the same location, count type, and date (e.g., different directions or time periods) into a single total count per day.

### Step 2: Adding Day of Week Information

A `day_of_week` column is added to enable day-specific analysis:

```julia
total_counts.day_of_week = dayname.(total_counts.count_date)
```

This extracts the day name (Monday, Tuesday, etc.) from each date, allowing for day-of-week specific calculations later in the pipeline.

### Step 3: Filtering for Relevant Count Types

The data is filtered to only include the three traffic types of interest:
- **P**: Pedestrians (walkers)
- **J**: Joggers
- **C**: Children (in carriers or walking)

```julia
relevant_counts = total_counts[[x in ["P", "J", "C"] for x in total_counts.count_type],:]
```

All other count types (B=bicycles, W=wheelchairs, S=skaters, O=other) are excluded from further processing.

### Step 4: Location Selection

Only locations that have data for **all three** count types (P, J, and C) are retained. This ensures that the weighted projection formula can be applied consistently across all locations.

The selection process:
1. Iterates through all unique location IDs
2. Checks if each location has at least one record for each of P, J, and C
3. Only includes locations where all three types are present

This filtering step is critical because the projection formula requires all three components to produce meaningful results.

## mean_counts.csv Generation

### Data Projection Formula

The core of the `mean_counts.csv` generation is a **weighted projection** that combines the three traffic types into a single metric. The projection formula is:

```
projected_count = mean(P) + 2 × mean(J) + 0.5 × mean(C)
```

Where:
- `mean(P)`: Average pedestrian count across all dates for the location
- `mean(J)`: Average jogger count across all dates for the location  
- `mean(C)`: Average child count across all dates for the location

**Weighting Rationale:**
- **Joggers (×2)**: Joggers are weighted twice as heavily as pedestrians, likely because jogging generates more energy per person for triboelectric tile applications
- **Children (×0.5)**: Children are weighted half as much, possibly due to lower average weight or different gait patterns
- **Pedestrians (×1)**: Baseline weight of 1.0

### Calculation Process

For each selected location:

1. **Separate counts by type**: Split the data into three subsets (pedestrians, joggers, children)

2. **Calculate overall average**: Compute the overall projected count using the formula above

3. **Calculate day-of-week specific averages**: For each day of the week (Monday through Sunday):
   - If data exists for that specific day and traffic type, use the day-specific mean
   - If no data exists for that day/type combination, fall back to the overall mean for that traffic type
   - Apply the projection formula using the day-specific or fallback means

4. **Output structure**: Create a row with:
   - `bp_loc_id`: Location identifier
   - `avg_count`: Overall projected average count
   - `Monday count`, `Tuesday count`, ..., `Sunday count`: Day-specific projected counts

### Example Calculation

For a location with:
- Pedestrian counts: [100, 120, 110] → mean = 110
- Jogger counts: [20, 25, 30] → mean = 25
- Child counts: [10, 8, 12] → mean = 10

**Overall projected count:**
```
110 + 2×25 + 0.5×10 = 110 + 50 + 5 = 165
```

**Day-specific calculation (e.g., Tuesday):**
If Tuesday has:
- Pedestrian mean: 115 (from Tuesday data)
- Jogger mean: 30 (from Tuesday data)  
- Child mean: 8 (from Tuesday data)

Then:
```
Tuesday count = 115 + 2×30 + 0.5×8 = 115 + 60 + 4 = 179
```

If Tuesday has no child data, it falls back to overall child mean (10):
```
Tuesday count = 115 + 2×30 + 0.5×10 = 115 + 60 + 5 = 180
```

## Output File Structure

The `mean_counts.csv` file contains the following columns:

| Column | Description |
|--------|-------------|
| `bp_loc_id` | Unique location identifier |
| `avg_count` | Overall weighted average projected count |
| `Monday count` | Projected count for Mondays |
| `Tuesday count` | Projected count for Tuesdays |
| `Wednesday count` | Projected count for Wednesdays |
| `Thursday count` | Projected count for Thursdays |
| `Friday count` | Projected count for Fridays |
| `Saturday count` | Projected count for Saturdays |
| `Sunday count` | Projected count for Sundays |

## Key Features

1. **Data Completeness**: Only locations with all three traffic types are included, ensuring consistent analysis
2. **Temporal Granularity**: Day-of-week specific counts allow for modeling weekly patterns
3. **Robust Fallback**: Missing day-specific data falls back to overall means, preventing data loss
4. **Weighted Projection**: The formula accounts for different energy generation potential of different traffic types

## Usage

The `mean_counts.csv` file is designed to be used as input for optimization models (such as the one in `model.ipynb`) that determine optimal placement of triboelectric tiles based on projected foot traffic patterns.


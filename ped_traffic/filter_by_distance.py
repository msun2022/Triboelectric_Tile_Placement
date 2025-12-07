import pandas as pd
import numpy as np

# Read bp_loc_id_str_name.csv
location_data = pd.read_csv('bp_loc_id_str_name.csv')

print(f"Original locations in bp_loc_id_str_name.csv: {len(location_data)}")

# Filter to only include locations with non-NaN distances
location_data_filtered = location_data[location_data['distance_to_city_hall_miles'].notna()].copy()

print(f"Locations with valid distances: {len(location_data_filtered)}")

# Get the list of location IDs with valid distances
valid_location_ids = set(location_data_filtered['bp_location_id'].unique())
print(f"Unique location IDs with valid distances: {len(valid_location_ids)}")

# Write filtered location data
location_data_filtered.to_csv('bp_loc_id_str_name.csv', index=False)
print(f"\nFiltered bp_loc_id_str_name.csv written (only locations with valid distances)")

# Read mean_counts.csv
mean_counts = pd.read_csv('ped_traffic/mean_counts.csv')

print(f"\nOriginal locations in mean_counts.csv: {len(mean_counts)}")

# Filter mean_counts to only include locations with valid distances
mean_counts_dist = mean_counts[mean_counts['bp_loc_id'].isin(valid_location_ids)].copy()

print(f"Locations in mean_counts with valid distances: {len(mean_counts_dist)}")

# Merge distance data from location_data_filtered
# Create a mapping of bp_location_id to distance
distance_map = location_data_filtered.set_index('bp_location_id')['distance_to_city_hall_miles'].to_dict()

# Add distance column to mean_counts_dist
mean_counts_dist['distance_to_city_hall_miles'] = mean_counts_dist['bp_loc_id'].map(distance_map)

# Write filtered mean_counts with distance column
mean_counts_dist.to_csv('mean_counts_dist.csv', index=False)

print(f"\nFiltered mean_counts written to mean_counts_dist.csv")
print(f"\nFirst few rows of mean_counts_dist:")
print(mean_counts_dist.head(10))


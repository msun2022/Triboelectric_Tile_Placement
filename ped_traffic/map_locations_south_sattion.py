import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import time

# Read mean_counts.csv to get the list of locations we care about
mean_counts_df = pd.read_csv("ped_traffic/mean_counts.csv")
locations_in_mean_counts = set(mean_counts_df["bp_loc_id"].unique())
print(f"Found {len(locations_in_mean_counts)} locations in mean_counts.csv")

# Read the bike_ped_counts.csv file
df = pd.read_csv(
    "ped_traffic/bike_ped_counts - Copy.csv", encoding="latin-1", low_memory=False
)
locations_in_bike_ped = set(df["bp_loc_id"].unique())
print(f"Found {len(locations_in_bike_ped)} locations in bike_ped_counts.csv")

# Find locations in mean_counts but not in bike_ped_counts
locations_not_in_bike_ped = locations_in_mean_counts - locations_in_bike_ped
print(
    f"\nLocations in mean_counts.csv but NOT in bike_ped_counts.csv: {len(locations_not_in_bike_ped)}"
)

# Filter to only include locations that are in mean_counts.csv
df_filtered = df[df["bp_loc_id"].isin(locations_in_mean_counts)]
print(f"Filtered bike_ped_counts.csv to {len(df_filtered)} rows")

# Get unique bp_loc_id and from_st_name pairs
# Use the first/most common street name per location for geocoding
location_mapping = (
    df_filtered.groupby("bp_loc_id")["from_st_name"].first().reset_index()
)
print(f"Unique locations after grouping: {len(location_mapping)}")
print(location_mapping.head())

# Sort by bp_loc_id
location_mapping = location_mapping.sort_values("bp_loc_id")
print(f"Processing {len(location_mapping)} unique locations...")

# Initialize geocoder
geolocator = Nominatim(user_agent="triboelectric_tile_placement")

# Geocode Boston South Station
print("Geocoding Boston South Station...")
south_station_address = "700 Atlantic Ave, Boston, MA 02110"
south_station_location = geolocator.geocode(south_station_address)
if south_station_location:
    south_station_coords = (
        south_station_location.latitude,
        south_station_location.longitude,
    )
    print(f"South Station coordinates: {south_station_coords}")

# Calculate distance for each location
print("\nGeocoding locations and calculating distances...")
distances = []
latitudes = []
longitudes = []
geocoded_count = 0

for idx, row in location_mapping.iterrows():
    street_name = row["from_st_name"]

    # Try to geocode the street name with Boston, MA
    address = f"{street_name}, Boston, MA"
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            street_coords = (location.latitude, location.longitude)
            # Calculate distance in miles
            distance_miles = geodesic(south_station_coords, street_coords).miles
            distances.append(distance_miles)
            latitudes.append(location.latitude)
            longitudes.append(location.longitude)
            geocoded_count += 1
        else:
            print(f"Warning: Could not geocode {street_name}, using NaN")
            distances.append(None)
            latitudes.append(None)
            longitudes.append(None)
    except Exception as e:
        print(f"Error geocoding {street_name}: {e}")
        distances.append(None)
        latitudes.append(None)
        longitudes.append(None)

    # Rate limiting - be nice to the geocoding service
    time.sleep(1)

    # Progress update
    if (idx + 1) % 10 == 0:
        print(f"Processed {idx + 1}/{len(location_mapping)} locations...")

# Add columns for coordinates and distance
location_mapping["latitude"] = latitudes
location_mapping["longitude"] = longitudes
location_mapping["distance_to_south_station_miles"] = distances

# Rename bp_loc_id to bp_location_id for consistency
location_mapping = location_mapping.rename(columns={"bp_loc_id": "bp_location_id"})

# Reorder columns: bp_location_id, from_st_name, latitude, longitude, distance_to_south_station_miles
location_mapping = location_mapping[
    [
        "bp_location_id",
        "from_st_name",
        "latitude",
        "longitude",
        "distance_to_south_station_miles",
    ]
]

print(f"\nSuccessfully geocoded {geocoded_count}/{len(location_mapping)} locations")

# Write to CSV
location_mapping.to_csv("bp_loc_id_str_name_ss.csv", index=False)

print(f"\nMapping written to bp_loc_id_str_name_ss.csv")
print(f"Total locations: {len(location_mapping)}")
print(f"\nFirst few rows:")
print(location_mapping.head(10))


### FILTER BY DISTANCE
# Read bp_loc_id_str_name_ss.csv
location_data = pd.read_csv("bp_loc_id_str_name_ss.csv")

print(f"Original locations in bp_loc_id_str_name_ss.csv: {len(location_data)}")

# Filter to only include locations with non-NaN distances
location_data_filtered = location_data[
    location_data["distance_to_south_station_miles"].notna()
].copy()

print(f"Locations with valid distances: {len(location_data_filtered)}")

# Get the list of location IDs with valid distances
valid_location_ids = set(location_data_filtered["bp_location_id"].unique())
print(f"Unique location IDs with valid distances: {len(valid_location_ids)}")

# Write filtered location data
location_data_filtered.to_csv("bp_loc_id_str_name_ss.csv", index=False)
print(
    f"\nFiltered bp_loc_id_str_name_ss.csv written (only locations with valid distances)"
)

# Read mean_counts.csv
mean_counts = pd.read_csv("ped_traffic/mean_counts.csv")

print(f"\nOriginal locations in mean_counts.csv: {len(mean_counts)}")

# Filter mean_counts to only include locations with valid distances
mean_counts_dist_ss = mean_counts[
    mean_counts["bp_loc_id"].isin(valid_location_ids)
].copy()

print(f"Locations in mean_counts with valid distances: {len(mean_counts_dist_ss)}")

# Merge distance data from location_data_filtered
# Create a mapping of bp_location_id to distance
distance_map = location_data_filtered.set_index("bp_location_id")[
    "distance_to_south_station_miles"
].to_dict()

# Add distance column to mean_counts_dist_ss
mean_counts_dist_ss["distance_to_south_station_miles"] = mean_counts_dist_ss[
    "bp_loc_id"
].map(distance_map)

# Write filtered mean_counts with distance column
mean_counts_dist_ss.to_csv("mean_counts_dist_ss.csv", index=False)

print(f"\nFiltered mean_counts written to mean_counts_dist_ss.csv")
print(f"\nFirst few rows of mean_counts_dist_ss:")
print(mean_counts_dist_ss.head(10))

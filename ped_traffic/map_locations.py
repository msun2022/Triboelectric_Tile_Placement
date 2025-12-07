import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import time

# Read mean_counts.csv to get the list of locations we care about
mean_counts_df = pd.read_csv('ped_traffic/mean_counts.csv')
locations_in_mean_counts = set(mean_counts_df['bp_loc_id'].unique())
print(f"Found {len(locations_in_mean_counts)} locations in mean_counts.csv")

# Read the bike_ped_counts.csv file
df = pd.read_csv('ped_traffic/bike_ped_counts - Copy.csv', encoding='latin-1', low_memory=False)
locations_in_bike_ped = set(df['bp_loc_id'].unique())
print(f"Found {len(locations_in_bike_ped)} locations in bike_ped_counts.csv")

# Find locations in mean_counts but not in bike_ped_counts
locations_not_in_bike_ped = locations_in_mean_counts - locations_in_bike_ped
print(f"\nLocations in mean_counts.csv but NOT in bike_ped_counts.csv: {len(locations_not_in_bike_ped)}")

# Filter to only include locations that are in mean_counts.csv
df_filtered = df[df['bp_loc_id'].isin(locations_in_mean_counts)]
print(f"Filtered bike_ped_counts.csv to {len(df_filtered)} rows")

# Get unique bp_loc_id and from_st_name pairs
# Use the first/most common street name per location for geocoding
location_mapping = df_filtered.groupby('bp_loc_id')['from_st_name'].first().reset_index()
print(f"Unique locations after grouping: {len(location_mapping)}")
print(location_mapping.head())

# Sort by bp_loc_id
location_mapping = location_mapping.sort_values('bp_loc_id')
print(f"Processing {len(location_mapping)} unique locations...")

# Initialize geocoder
geolocator = Nominatim(user_agent="triboelectric_tile_placement")

# Geocode Boston City Hall
print("Geocoding Boston City Hall...")
city_hall_address = "1 City Hall Square, Boston, MA 02201"
city_hall_location = geolocator.geocode(city_hall_address)
if city_hall_location:
    city_hall_coords = (city_hall_location.latitude, city_hall_location.longitude)
    print(f"City Hall coordinates: {city_hall_coords}")
else:
    print("Error: Could not geocode City Hall")
    city_hall_coords = (42.3601, -71.0589)  # Fallback coordinates

# Calculate distance for each location
print("\nGeocoding locations and calculating distances...")
distances = []
latitudes = []
longitudes = []
geocoded_count = 0

for idx, row in location_mapping.iterrows():
    street_name = row['from_st_name']
    
    # Try to geocode the street name with Boston, MA
    address = f"{street_name}, Boston, MA"
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            street_coords = (location.latitude, location.longitude)
            # Calculate distance in miles
            distance_miles = geodesic(city_hall_coords, street_coords).miles
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
location_mapping['latitude'] = latitudes
location_mapping['longitude'] = longitudes
location_mapping['distance_to_city_hall_miles'] = distances

# Rename bp_loc_id to bp_location_id for consistency
location_mapping = location_mapping.rename(columns={'bp_loc_id': 'bp_location_id'})

# Reorder columns: bp_location_id, from_st_name, latitude, longitude, distance_to_city_hall_miles
location_mapping = location_mapping[['bp_location_id', 'from_st_name', 'latitude', 'longitude', 'distance_to_city_hall_miles']]

print(f"\nSuccessfully geocoded {geocoded_count}/{len(location_mapping)} locations")

# Write to CSV
location_mapping.to_csv('bp_loc_id_str_name.csv', index=False)

print(f"\nMapping written to bp_loc_id_str_name.csv")
print(f"Total locations: {len(location_mapping)}")
print(f"\nFirst few rows:")
print(location_mapping.head(10))


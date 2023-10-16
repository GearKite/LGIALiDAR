import json
import os

# Initialize an empty list to store file paths
file_paths = []

# Walk through the directory tree and collect file paths
for root, directories, files in os.walk("./output/missing_buildings/"):
    for filename in files:
        file_paths.append(os.path.join(root, filename))
        
for path in file_paths:
    # Open the GeoJSON file for reading
    with open(path, 'r') as geojson_file:
        geojson_data = json.load(geojson_file)

    # Iterate through features and swap coordinates
    for feature in geojson_data['features']:
        coordinates = feature['geometry']['coordinates']
        coordinates[0], coordinates[1] = coordinates[1], coordinates[0]

    # Save the corrected GeoJSON
    with open(path, 'w') as corrected_file:
        json.dump(geojson_data, corrected_file, indent=2)
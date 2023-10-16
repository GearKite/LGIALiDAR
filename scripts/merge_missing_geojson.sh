#!/bin/bash

# Set the source directory where you have subdirectories containing .tif files
src_dir="./output/missing_buildings"

out_dir="./output/missing_buildings_merged"

# Create a target directory for merged files
mkdir -p $out_dir

# Semaphore to limit parallel processes
max_parallel_processes=2
semaphore=0

# Function to merge .tif files in a directory
merge_directory() {
    local dir="$1"
    local dir_name="$(basename "$dir")"
    local merged_file="$out_dir/$dir_name.geojson"
    
    # Check if the merged file already exists
    if [ ! -f "$merged_file" ]; then
        echo Merging $dir_name
        geojson-merge "$dir"/*.geojson > "$merged_file"
    else
        echo "Merged file '$merged_file' already exists. Skipping directory '$dir_name'."
    fi
}

# Iterate through subdirectories and run merge commands in parallel with semaphore
for dir in "$src_dir"/*; do
    if [ -d "$dir" ]; then
        # Check if we've reached the maximum parallel processes
        if [ "$semaphore" -ge "$max_parallel_processes" ]; then
            # Wait for one of the background jobs to finish
            wait -n
            semaphore=$((semaphore-1))
        fi
        
        # Run the merge_directory function in the background
        merge_directory "$dir" &
        semaphore=$((semaphore+1))
    fi
done

# Wait for all background jobs to finish
wait

geojson-merge "$out_dir"/*.geojson > "$out_dir"/merged.geojson

ogr2ogr -f "ESRI Shapefile" output/missing_buildings.shp output/missing_buildings_merged/merged.geojson
# Use find to locate all files with "_compressed" in their names
find "." -type f -name '*_compressed*' | while read -r compressed_file; do
    # Remove the "_compressed" suffix from the file name
    new_name="${compressed_file//_compressed/}"
    
    # Rename the file
    mv "$compressed_file" "$new_name"
    
    echo "Renamed: $compressed_file -> $new_name"
done
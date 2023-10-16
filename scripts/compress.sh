for file in $(find . -type f -iname "*.tif" ! -name "*_compressed.tif"); do
   output_file="${file%.tif}_compressed.tif"
   gdal_translate -of GTiff -co "COMPRESS=ZSTD" "$file" "$output_file"
   echo "Compressed: $file -> $output_file"
   rm "$file"  # Remove the original uncompressed file
   echo "Deleted: $file"
done

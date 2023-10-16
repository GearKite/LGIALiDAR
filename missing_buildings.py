# %%
from osgeo import gdal
import numpy as np
import psycopg2
from osgeo import osr
import pyproj
import os
os.environ['USE_PYGEOS'] = '0'
import multiprocessing
import osmium
import geopandas as gpd
from shapely.geometry import Polygon, Point
from skimage import measure
import geojson
import yaml

# Load config
with open("./config.yaml") as f:
    config = yaml.safe_load(f)["missing_buildings"]

# Define the source CRS and target CRS
source_crs = pyproj.CRS("EPSG:3059")
target_crs = pyproj.CRS("EPSG:4326")

# Create a PyProj transformer
transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

# Handler for OSM data
class BuildingHandler(osmium.SimpleHandler):
    def __init__(self):
        super(BuildingHandler, self).__init__()
        self.buildings = []

    # Handle simple polygon
    def way(self, way):
        if 'building' in way.tags:
            polygon = Polygon([(node.lon, node.lat) for node in way.nodes])
            self.buildings.append(polygon)

    # Handle multipolygons
    def relation(self, relation):
        if 'building' in relation.tags and 'type' in relation.tags:
            if relation.tags['type'] == 'multipolygon':
                multipolygon = self.extract_multipolygon(relation)
                if multipolygon:
                    self.buildings.append(multipolygon)

    def create_polygon(self, nodes):
        if len(nodes) < 4:
            return None  # Skip ways with insufficient coordinates

        return Polygon([(node.lon, node.lat) for node in nodes])

    def extract_multipolygon(self, relation):
        outer_ring = None
        inner_rings = []

        for member in relation.members:
            if member.role == 'outer' and isinstance(member, osmium.osm.Way):
                outer_ring = self.create_polygon(member.nodes)
            elif member.role == 'inner' and isinstance(member, osmium.osm.Way):
                inner_ring = self.create_polygon(member.nodes)
                if inner_ring:
                    inner_rings.append(inner_ring)

        if outer_ring:
            # Construct the multipolygon from the outer and inner rings
            multipolygon = Polygon(outer_ring.exterior.coords, inner_rings)
            return multipolygon

# Load OSM data
osm_file = './dataset/extract.osm.pbf'
handler = BuildingHandler()
handler.apply_file(osm_file, locations=True)

# Create a GeoDataFrame from the building polygons
gdf = gpd.GeoDataFrame({'geometry': handler.buildings})

# Define a function to calculate the center pixel of a labeled group
def calculate_group_center(indices):
    # Calculate the center as the mean of indices
    center = np.median(indices, axis=0).astype(int)
    center_x, center_y = center[0], center[1]

    return center_x, center_y
# %%
def process_file(geotiff_path, output_tiff_path, output_geojson_path):
    try:
        print(f"Processing {geotiff_path}")
        ds = gdal.Open(geotiff_path)

        if ds is None:
            print(f"Failed to open the GeoTIFF file: {geotiff_path}")
            return

        projection = ds.GetProjection()
        geotransform = ds.GetGeoTransform()
        width = ds.RasterXSize
        height = ds.RasterYSize

        # Read the entire GeoTIFF into a NumPy array
        geotiff_array = ds.ReadAsArray()

        # Create an output array
        output_array = np.zeros_like(geotiff_array, dtype=np.uint8)

        # Label connected pixels to get a list of buildings
        buildings_in_image, num_buildings = measure.label(geotiff_array, connectivity=2, background=0, return_num=True)
        
        features = []
        
        for building_label in range(1, num_buildings + 1):
            #print(f"{building_label}/{num_buildings}")
            # Find the indices where the label matches
            indices = np.argwhere(buildings_in_image == building_label)
            
            # Calculate the center pixel of the current building group
            center_y, center_x = calculate_group_center(indices)
            
            # Convert center coordinates to geographic coordinates
            center_lon_s = geotransform[0] + center_x * geotransform[1]
            center_lat_s = geotransform[3] + center_y * geotransform[5]
            
            # Perform the transformation
            center_lon_t, center_lat_t = transformer.transform(center_lon_s, center_lat_s)

            # Check if the center coordinate is inside a building
            center_inside_building = gdf.geometry.contains(Point(center_lon_t, center_lat_t)).any()
            
            if not center_inside_building:
                output_array[center_y, center_x] = 255
                features.append(geojson.Feature(
                        geometry=geojson.Point((center_lon_t, center_lat_t))
                    ))

        # Save the output array as a GeoTIFF
        if config["enable_output_geotiff"]:
            driver = gdal.GetDriverByName('GTiff')
            output_ds = driver.Create(output_path, width, height, 1, gdal.GDT_Byte, options=["COMPRESS=ZSTD", "TILED=YES"])
            output_ds.SetGeoTransform(geotransform)
            output_ds.SetProjection(projection)
            output_ds.GetRasterBand(1).WriteArray(output_array)
            output_ds.GetRasterBand(1).SetNoDataValue(0)
            output_ds.FlushCache()
        
        if config["enable_output_geojson"]:
            feature_collection = geojson.FeatureCollection(features)

            # Define the output GeoJSON file path
            output_file_path = output_path.replace(".tif", ".geojson")

            # Write the FeatureCollection to a GeoJSON file
            with open(output_file_path, 'w+') as f:
                geojson.dump(feature_collection, f)
        
        print(f"Processed {geotiff_path} and saved to {output_path}")

    except Exception as e:
        print(f"Error processing {geotiff_path}: {str(e)}")

# %%
def process_file_manager(processing_queue):
    while True:
        try:
            geotiff_path, filename = processing_queue.get(block=True)
            
            if geotiff_path is None:
                break
            
            if os.path.exists(output_path):
                print(f"Skipping {output_path}")
                continue
            
            output_path = os.path.join(output_dir, os.path.basename(os.path.dirname(geotiff_path)),filename)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            process_file(geotiff_path, output_path)

        except Exception as e:
            print(f"Error in process_file_manager: {str(e)}")


# %%
if __name__ == '__main__':
    num_workers = 8  # Adjust the number of workers as needed
    processing_queue = multiprocessing.Queue()
    
    # Initialize an empty list to store file paths
    file_paths = []

    # Walk through the directory tree and collect file paths
    for root, directories, files in os.walk(input_geotiff_dir):
        for filename in files:
            file_paths.append(os.path.join(root, filename))


    # Populate the processing queue
    for geotiff_path in file_paths:
        output_path = os.path.join(output_dir, os.path.basename(geotiff_path)).replace('.tif',".geojson")
        filename = os.path.basename(geotiff_path)
        if not os.path.exists(output_path):
            processing_queue.put((geotiff_path, filename))

    # Add termination signals to the queue
    for _ in range(num_workers):
        processing_queue.put((None, None))
        
    #process_file_manager(processing_queue)

    # Start worker processes
    processes = []
    for _ in range(num_workers):
        process = multiprocessing.Process(target=process_file_manager, args=(processing_queue,))
        process.start()
        processes.append(process)

    # Wait for all processes to finish
    for process in processes:
        process.join()


# %%

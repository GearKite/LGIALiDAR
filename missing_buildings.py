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


# %%
# Define the source CRS and target CRS
source_crs = pyproj.CRS("3059")  # LKS-92
target_crs = pyproj.CRS("EPSG:4326")  # WGS 84

geotiff_dir = "./output/buildings_merged"
output_dir = "./output/missing_buildings"

# Create a PyProj transformer
transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

# %%

class BuildingHandler(osmium.SimpleHandler):
    def __init__(self):
        super(BuildingHandler, self).__init__()
        self.buildings = []

    def way(self, w):
        if 'building' in w.tags:
            polygon = Polygon([(n.lon, n.lat) for n in w.nodes])
            self.buildings.append(polygon)


# %%
# Replace 'your_osm_file.osm' with the path to your OSM data file
osm_file = './dataset/extract.osm.pbf'
handler = BuildingHandler()
handler.apply_file(osm_file, locations=True)

# Create a GeoDataFrame from the building polygons
gdf = gpd.GeoDataFrame({'geometry': handler.buildings})
dpgeometry = gdf.geometry


# %%
def process_file(geotiff_path, output_path):
    if os.path.exists(output_path):
        print(f"Skipping {output_path}")
        return
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

        # Create a spatial index for building polygons and use it to optimize queries
        #cursor.execute("CREATE INDEX IF NOT EXISTS building_geom_idx ON planet_osm_polygon USING GIST(way);")
        

        for y in range(height):
            for x in range(width):
                pixel_value = geotiff_array[y, x]

                if pixel_value == 0:
                    continue
                
                coordx = geotransform[0] + x * geotransform[1]
                coordy = geotransform[3] + y * geotransform[5]
                lat, lon = transformer.transform(coordx, coordy)

                inside_building = dpgeometry.contains(Point(lat, lon)).any()
                
                if not inside_building:
                    output_array[y, x] = 255

            print(f"{y}/{height} complete for {geotiff_path}")

        # Use a spatial filter to remove small connected components
        # Label connected components in the output array
        labeled_components, num_components = measure.label(output_array, connectivity=1, background=0, return_num=True)

        # Create a dictionary to count the number of pixels in each component
        component_sizes = {}

        # Iterate through connected components to count their sizes
        for component_label in range(1, num_components + 1):
            component_mask = (labeled_components == component_label)
            component_size = np.sum(component_mask)
            component_sizes[component_label] = component_size

        # Iterate through connected components and filter out those with < 5 pixels
        for component_label in range(1, num_components + 1):
            if component_sizes[component_label] <= 5:
                output_array[labeled_components == component_label] = 0

        # Save the output array as a GeoTIFF
        driver = gdal.GetDriverByName('GTiff')
        output_ds = driver.Create(output_path, width, height, 1, gdal.GDT_Byte, options=["COMPRESS=ZSTD", "TILED=YES"])
        output_ds.SetGeoTransform(geotransform)
        output_ds.SetProjection(projection)
        output_ds.GetRasterBand(1).WriteArray(output_array)
        output_ds.GetRasterBand(1).SetNoDataValue(0)
        output_ds.FlushCache()

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

            output_path = os.path.join(output_dir, filename)
            process_file(geotiff_path, output_path)

        except Exception as e:
            print(f"Error in process_file_manager: {str(e)}")


# %%
if __name__ == '__main__':
    num_workers = 1  # Adjust the number of workers as needed
    processing_queue = multiprocessing.Queue()

    # Populate the processing queue
    for filename in os.listdir(geotiff_dir):
        geotiff_path = os.path.join(geotiff_dir, filename)
        output_path = os.path.join(output_dir, filename)
        if not os.path.exists(output_path):
            processing_queue.put((geotiff_path, filename))

    # Add termination signals to the queue
    for _ in range(num_workers):
        processing_queue.put((None, None))
        
    process_file_manager(processing_queue)

    # Start worker processes
    #processes = []
    #for _ in range(num_workers):
    #    process = multiprocessing.Process(target=process_file_manager, args=(processing_queue,))
    #    process.start()
    #    processes.append(process)

    # Wait for all processes to finish
    #for process in processes:
    #    process.join()



# %%

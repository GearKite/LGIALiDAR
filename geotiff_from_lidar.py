import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio import features
from pyproj import CRS, Transformer
import pylas
import json
import requests
import os
import queue
import multiprocessing
import time
import laspy
import yaml
import traceback
import subprocess
import logging
import coloredlogs

log = logging.getLogger(__name__)
log.level = logging.DEBUG

coloredlogs.install(level="DEBUG", logger=log)

# Load config
with open("./config.yaml") as f:
    config = yaml.safe_load(f)["geotiff_from_lidar"]

# Define the CRS for the input point cloud (CRS 3059)
CRS = CRS.from_epsg(3059)

# Create queues for multiprocessing
download_queue = multiprocessing.Queue()
processing_queue = multiprocessing.Queue()

download_queue_urls = []

def main():
    # Get a list of all LAS files
    files_to_be_downloaded = get_todo()
    for url in files_to_be_downloaded:
        las_filename = url.split("/las/")[1]
        las_path = os.path.join(config["processing"]["las_path"], las_filename)
        tiff_filename = las_filename.replace(".las", ".tif")

        for generator in config["outputs"]:
            if not generator["enabled"]:
                continue
            
            output_tiff_path = os.path.join(generator["path"], tiff_filename)
            if check_can_continue_processing(las_path, output_tiff_path, tiff_filename, url):
                processing_queue.put((las_path, tiff_filename, url))

def download_manager():
    while True:
        try:
            url, save_path, tiff_filename = download_queue.get()
            download_file(url, save_path, tiff_filename)
        except:
            log.error(f"Could not download {save_path} from {url}", exc_info=True)

def download_file(url, save_path, tiff_filename):
    try:
        log.info(f"Downloading {save_path}")
        # Create directory if it does not exist
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        # Download file
        os.system(f"wget -O {save_path} {url}") # wget is faster
        '''
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        chunk_size = 1024 * 32
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    file.write(chunk)
        print(f"Downloaded {save_path}")'
        '''
        # Add file to processing queue
        processing_queue.put((save_path, tiff_filename, url))
        log.debug(f"Added file to processing queue {save_path}")
    except:
        log.error(f"Error downloading {url}", exc_info=True)


def get_todo():
    las_urls = requests.get("https://s3.storage.pub.lvdc.gov.lv/lgia-opendata/las/LGIA_OpenData_las_saites.txt").text.split("\r\n")
    return las_urls

def process_file_manager():
    while True:
        try:
            input_path, tiff_filename, url = processing_queue.get()
            
            # Get path for LAZ file. There probably is a better way to do this.
            laz_path = os.path.join(config["processing"]["laz_path"], os.path.basename(os.path.dirname(input_path)), os.path.basename(input_path).replace(".las",".laz"))
            
            for generator in config["outputs"]:
                if not generator["enabled"]:
                    continue
                
                output_tiff_path = os.path.join(generator["path"], tiff_filename)
                if not check_can_continue_processing(input_path, output_tiff_path, tiff_filename, url):
                    continue
                
                log.info(f"Generating {generator['type']} raster from {input_path}")

                if generator["type"] == "color":
                    generate_color_raster(input_path, tiff_filename, generator, url)
                elif generator["type"] == "binary":
                    generate_binary_raster(input_path, tiff_filename, generator, url)
                elif generator["type"] == "linear":
                    generate_linear_raster(input_path, tiff_filename, generator, url)
            # After processnig compress the LAS file for safe keeping (and later usage)
            if config["processing"]["compress_to_laz"]:
                try:
                    if os.path.isfile(input_path):
                        compress_las(input_path)
                except:
                    log.warning(f"Could not compress {input_path}, skipping")
            
            if config["processing"]["delete_las_after_processing"]:
                if os.path.isfile(input_path):
                    log.debug(f"Deleting {input_path}")
                    os.remove(input_path)
        # This usually happens if downloading or compression get interrupted
        except pylas.errors.PylasError:
            log.warning(f"Redownloading {input_path}")
            redownload_file(input_path, tiff_filename, url)
        except Exception:
            log.error(f"Could not render {input_path} to {tiff_filename}! Deleting LAS file and adding it back to queue")
            log.error(f"Unknown exception", exc_info=True)

def redownload_file(las_file, tiff_filename, url):
    try:
        os.remove(las_file)
    except FileNotFoundError as e:
        pass
    laz_path = os.path.join(config["processing"]["laz_path"], os.path.basename(os.path.dirname(las_file)), os.path.basename(las_file).replace(".las",".laz"))
    if os.path.isfile(laz_path):
        os.remove(laz_path)
    if url not in download_queue_urls:
        download_queue_urls.append(url)
        download_queue.put((url, las_file, tiff_filename))

def compress_las(las_path):
    # Read the input LAS file
    las = laspy.read(las_path)

    # Create a new LasData object for the output file with compression enabled
    output_las = laspy.LasData(las.header)

    # Copy all points from the input LasData object to the output LasData object
    output_las.points = las.points.copy()
    
    # Extract the last directory name from the first input path
    last_directory = os.path.basename(os.path.dirname(las_path))

    # Create the first output path by joining the second input path with the last directory
    laz_path_dir = os.path.join(config["processing"]["laz_path"], last_directory)

    # Create the second output path by joining the first output path with the file name
    laz_path = os.path.join(laz_path_dir, os.path.basename(las_path).replace(".las", ".laz"))
    
    if os.path.isfile(laz_path):
        log.debug(f"Not compressing {las_path} because it has already been compressed")
        return
    
    log.debug(f"Compressing {las_path} to {laz_path}")
    
    os.makedirs(laz_path_dir, exist_ok=True)

    with open(laz_path, "wb+") as compressed_file:  
    
        # Write the compressed LAZ file
        output_las.write(compressed_file, do_compress=True)

def decompress_laz(laz_path, las_path):
    # Need to use laszip, because lazrs is currently broken. When it's fixed, there should be no need for decompressing the laz file.
    os.system(f"./laszip -i {laz_path} -o {las_path}")

def check_can_continue_processing(input_path, tiff_path, tiff_filename, url):
    if os.path.isfile(tiff_path):
        log.debug(f"Skipping {tiff_path} because it has already been rasterized")
        return False
    
    laz_path = os.path.join(config["processing"]["laz_path"], os.path.basename(os.path.dirname(input_path)), os.path.basename(input_path).replace(".las",".laz"))
    # Check if LAS files need to be downloaded
    if os.path.isfile(input_path):
        return True
        
    # Check if a compressed LAZ version exists
    if os.path.isfile(laz_path):
        log.info(f"Decompressing {laz_path} to {input_path}")
        decompress_laz(laz_path, input_path)
        # LAZ files get corrupted if the compression process is interrupted
        if not os.path.isfile(input_path):
            log.warning(f"Could not decompress {laz_path}. Deleting it and adding it back to the queue...")
            redownload_file(input_path, tiff_filename, url)
            return False
    else:
        if url not in download_queue_urls:
            log.debug(f"Added LAS file for {tiff_filename} to downlaod queue")
            download_queue_urls.append(url)
            download_queue.put((url, input_path, tiff_filename))
        return False
    
    return True

def generate_color_raster(input_path, tiff_filename, options, url):
    output_tiff_path = os.path.join(options["path"], tiff_filename)
    
    if not check_can_continue_processing(input_path, output_tiff_path, tiff_filename, url):
        return
    
    os.makedirs(os.path.dirname(output_tiff_path), exist_ok=True)

    try:
        # Load the LIDAR file
        las_file = pylas.read(input_path)
    except:
        redownload_file(input_path, tiff_filename, url)
        return

    # Extract point cloud data
    x = np.array(las_file.x).astype(int)
    y = np.array(las_file.y).astype(int)
    classification = np.array(las_file.classification).astype(int)

    # Determine the raster dimensions and create an array to store pixel colors
    min_x, min_y, max_x, max_y = x.min(), y.min(), x.max(), y.max()
    img_width = max_x - min_x + 1
    img_height = max_y - min_y + 1

    # Increase resolution for higher quality
    output_resolution = 1
    img_width *= int(1 / output_resolution)
    img_height *= int(1 / output_resolution)
    
    img = np.zeros((img_height, img_width, 4), dtype=np.uint8)

    # Map classifications to colors
    colors = np.array([options["color_map"].get(c, (255, 255, 255)) for c in classification])

    # Calculate the pixel coordinates for each point
    px = x - min_x
    py = max_y - y

    # Assign colors to the corresponding pixels
    img[py, px, :3] = colors  # RGB channels
    img[py, px, 3] = 255  # Opaque background

    # Create a GeoTIFF with proper scaling
    transform = from_origin(min_x, max_y, 1, 1)  # Adjust resolution if needed
    with rasterio.open(output_tiff_path, 'w', driver='GTiff', width=img_width, height=img_height, count=4, dtype='uint8', crs=CRS, transform=transform, compress='ZSTD') as dst:
        dst.write(img.transpose(2, 0, 1))


    log.info(f"Generated color GeoTIFF {output_tiff_path}")
    

def generate_binary_raster(input_path, tiff_filename, options, url):
    output_tiff_path = os.path.join(options["path"], tiff_filename)
    
    if not check_can_continue_processing(input_path, output_tiff_path, tiff_filename, url):
        return
    
    os.makedirs(os.path.dirname(output_tiff_path), exist_ok=True)
    
    try:
        # Load the LIDAR file
        las_file = pylas.read(input_path)
    except:
        redownload_file(input_path, tiff_filename, url)
        return

    # Extract point cloud data
    x = np.array(las_file.x).astype(int)
    y = np.array(las_file.y).astype(int)
    classification = np.array(las_file.classification).astype(int)
    
    # Filter points with specific classification
    class_points = (classification == options["point_class"])
    
    # Determine the raster dimensions based on the bounding box of all points
    min_x, min_y, max_x, max_y = x.min(), y.min(), x.max(), y.max()
    img_width = max_x - min_x + 1
    img_height = max_y - min_y + 1
    
    img = np.zeros((img_height, img_width), dtype=np.uint8)
    
    # Check if there are any building points
    if np.any(class_points):
        # Extract coordinates of building points
        x_building = x[class_points]
        y_building = y[class_points]

        # Calculate the pixel coordinates for each building point within the bounding box
        px_building = x_building - min_x
        py_building = y_building - min_y

        # Assign a value of 255 (white) to the corresponding pixels for building points
        img[py_building, px_building] = 255

    # Create a GeoTIFF with corrected georeferencing information
    transform = from_origin(min_x, min_y, 1, -1)  # Adjust resolution if needed

    with rasterio.open(output_tiff_path, 'w', driver='GTiff', width=img_width, height=img_height, count=1, dtype='uint8', crs=CRS, transform=transform, compress='ZSTD', nodata=0) as dst:
        dst.write(img, 1)  # 1 is the band index
        
    log.info(f"Generated single bit GeoTIFF {output_tiff_path}")

def generate_linear_raster(input_path, tiff_filename, options, url):
    output_tiff_path = os.path.join(options["path"], tiff_filename)
    
    if not check_can_continue_processing(input_path, output_tiff_path, tiff_filename, url):
        return
    
    os.makedirs(os.path.dirname(output_tiff_path), exist_ok=True)
    
    try:
        # Load the LIDAR file
        las_file = pylas.read(input_path)
    except:
        redownload_file(input_path, tiff_filename, url)
        return

    # Extract point cloud data
    x = np.array(las_file.x).astype(int)
    y = np.array(las_file.y).astype(int)
    z = np.array(getattr(las_file, options["value_name"])).astype(np.int16)

    # Determine the raster dimensions
    min_x, min_y, max_x, max_y = min(x), min(y), max(x), max(y)
    img_width = max_x - min_x + 1
    img_height = max_y - min_y + 1
    
    # Apply linear scaling to Z values to map them to 8-bit range (0-255)
    scaled_z = ((z - options["min_value"]) / (options["max_value"] - options["min_value"]) * 255).astype(np.uint8)

    img = np.zeros((img_height, img_width), dtype=np.uint8)

    # Calculate the pixel coordinates for each point
    px = x - min_x
    py = y - min_y
    
    # Clip values
    scaled_z = np.clip(scaled_z, 0, 255)

    # Assign Z values to the corresponding pixels
    img[py, px] = scaled_z

    # Create a GeoTIFF with the Z values
    transform = from_origin(min_x, min_y, 1, -1)  # Adjust resolution if needed
    with rasterio.open(output_tiff_path, 'w', driver='GTiff', width=img_width, height=img_height, count=1, dtype='uint8', crs=CRS, transform=transform, compress='ZSTD') as dst:
        dst.write(img, 1)  # 1 is the band index

    
    log.info(f"Generated linear GeoTIFF {output_tiff_path}")
    
if __name__ == "__main__":
    main()
    
    # Start processes
    for _ in range(config["processing"]["processing_processes"]):
        process = multiprocessing.Process(target=process_file_manager, args=())
        process.start()
    
    for _ in range(config["processing"]["download_processes"]):
        process = multiprocessing.Process(target=download_manager, args=())
        process.start()
        time.sleep(1)
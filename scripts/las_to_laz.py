import laspy
from flask import Flask, request, send_file
import requests
from io import BytesIO
import requests_cache

app = Flask(__name__)

requests_cache.install_cache('las_cache', expire_after=120)

# Route for processing and forwarding the file
@app.route('/convert-to-laz', methods=['GET'])
def compress_las_to_laz_http():
    try:
        url = request.args.get('url')

        if not url:
            return "Missing 'url' in the request.", 400

        print(f"Downloading file: {url}")
        
        chunk_size = 1024 * 1024 * 10  # 10MB chunk size
        save_path = "temp/" + url.split("/las/")[1].replace("/", "-")
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    print(".")
                    file.write(chunk)
        
        print(f"Converting file: {url}")

        # Read the input LAS file
        las = laspy.read(save_path)

        # Create a new LasData object for the output file with compression enabled
        output_las = laspy.LasData(las.header)

        # Copy all points from the input LasData object to the output LasData object
        output_las.points = las.points.copy()

        compressed_file = BytesIO()
        
        # Set a filename
        compressed_file.filename = "compressed.laz"
        
        # Write the compressed LAZ file
        output_las.write(compressed_file, do_compress=True)

        return send_file(
            compressed_file,
            as_attachment=False,
            download_name='compressed.laz',
            mimetype='application/octet-stream'
        )
    except Exception as e:
        print(e)
        return str(e), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

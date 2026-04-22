

# pipeline_utils.py

import requests
import zipfile
import os

def download_zip(url, output_path):
    if os.path.exists(output_path):
        print("ZIP already exists, skipping download")
        return

    print("Downloading...")
    r = requests.get(url, stream=True)

    with open(output_path, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)

def extract_zip(zip_path, extract_to):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_to)

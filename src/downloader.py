import os
import zipfile
import logging
import urllib.request

logger = logging.getLogger("pipeline")

def download_and_extract_dataset(
    url: str,
    dest_dir: str,
    zip_name: str = "air_quality.zip",
    csv_name: str = "Air_Quality_Continuous.csv"
) -> str:
    """
    Downloads the official UWE Bristol Air Quality dataset ZIP archive and extracts it.
    
    Uses standard library urllib.request to fetch the archive and zipfile to unpack it.
    If the CSV dataset already exists, downloading is skipped. If the extracted CSV
    filename does not match `csv_name`, it is automatically renamed to maintain pipeline consistency.
    
    Parameters:
        url (str): Remote HTTP/HTTPS URL from which the ZIP archive is retrieved.
        dest_dir (str): Local filesystem path where files are extracted.
        zip_name (str): Temporary local name for the downloaded ZIP file.
        csv_name (str): Expected canonical name of the extracted CSV file.
        
    Returns:
        str: Absolute filesystem path to the extracted CSV file.
        
    Raises:
        FileNotFoundError: If the ZIP archive is successfully extracted but contains no CSV files.
        URLError / HTTPError: If connection issues occur during HTTP download.
    """
    os.makedirs(dest_dir, exist_ok=True)
    zip_path = os.path.join(dest_dir, zip_name)
    csv_path = os.path.join(dest_dir, csv_name)
    
    if os.path.exists(csv_path):
        logger.info(f"Dataset already exists at {csv_path}. Skipping download.")
        return csv_path
        
    logger.info(f"Downloading dataset from {url}...")
    try:
        # Custom headers to avoid basic scraping blocks
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        )
        with urllib.request.urlopen(req) as response, open(zip_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
        logger.info("Download completed successfully.")
        
        # Unzip
        logger.info(f"Extracting {zip_path} to {dest_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_dir)
            
        # Clean up zip
        if os.path.exists(zip_path):
            os.remove(zip_path)
            
        # Check if file name matches expected or rename if necessary
        # The file inside the zip might be named differently (e.g. lowercase or slightly different spelling)
        extracted_files = os.listdir(dest_dir)
        logger.info(f"Extracted files: {extracted_files}")
        
        for file in extracted_files:
            if file.lower().endswith('.csv'):
                actual_path = os.path.join(dest_dir, file)
                if file != csv_name:
                    os.rename(actual_path, csv_path)
                    logger.info(f"Renamed {file} to {csv_name}")
                return csv_path
                
        raise FileNotFoundError("No CSV file found in the extracted archive.")
        
    except Exception as e:
        logger.error(f"Failed to download/extract dataset: {e}")
        raise e

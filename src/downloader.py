import zipfile
import logging
import urllib.request
from pathlib import Path

logger = logging.getLogger("pipeline")

def download_and_extract_dataset(
    url: str,
    dest_dir: Path,
    zip_name: str = "air_quality.zip",
    csv_name: str = "air_quality_data_continuous.csv"
) -> Path:
    """
    Downloads the official UWE Bristol Air Quality dataset ZIP archive and extracts it.
    
    Uses standard library urllib.request to fetch the archive and zipfile to unpack it.
    If the CSV dataset already exists, downloading is skipped. If the extracted CSV
    filename does not match `csv_name`, it is automatically renamed to maintain pipeline consistency.
    
    Parameters:
        url (str): Remote HTTP/HTTPS URL from which the ZIP archive is retrieved.
        dest_dir (Path): Local filesystem path where files are extracted.
        zip_name (str): Temporary local name for the downloaded ZIP file.
        csv_name (str): Expected canonical name of the extracted CSV file.
        
    Returns:
        Path: Absolute filesystem path to the extracted CSV file.
        
    Raises:
        FileNotFoundError: If the ZIP archive is successfully extracted but contains no CSV files.
        URLError / HTTPError: If connection issues occur during HTTP download.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / zip_name
    csv_path = dest_dir / csv_name
    
    if csv_path.exists():
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
        if zip_path.exists():
            zip_path.unlink()
            
        # Check if file name matches expected or rename if necessary
        extracted_files = [f for f in dest_dir.iterdir() if f.is_file()]
        logger.info(f"Extracted files: {[f.name for f in extracted_files]}")
        
        for file in extracted_files:
            if file.suffix.lower() == '.csv':
                if file.name != csv_name:
                    file.rename(csv_path)
                    logger.info(f"Renamed {file.name} to {csv_name}")
                return csv_path
                
        raise FileNotFoundError("No CSV file found in the extracted archive.")
        
    except Exception as e:
        logger.error(f"Failed to download/extract dataset: {e}")
        raise e

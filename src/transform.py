import logging
from datetime import datetime
from typing import Any, List, Dict
from src.validate import validate_row_schema, compute_row_checksum, send_alert_notification

logger = logging.getLogger("pipeline")

def transform_batch(batch: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Cleanses, crops, validates, and hashes a batch of raw records in memory.
    
    This function parses the date strings into native Python datetime objects, filters
    them using the chronological crop limits defined in config.yaml, delegates quality
    checks to the schema validation engine, and computes a data-integrity MD5 checksum.
    Invalid or out-of-bounds readings are logged and dropped to keep the OLAP database clean.
    
    Parameters:
        batch (List[Dict[str, Any]]): List of raw observations retrieved from raw staging.
        config (Dict[str, Any]): Global pipeline configuration dictionary.
        
    Returns:
        List[Dict[str, Any]]: List of cleansed, validated, and checksum-hashed observations.
    """
    etl_config = config.get("etl", {})
    start_str = etl_config.get("crop_start_date", "2010-01-01")
    end_str = etl_config.get("crop_end_date", "2022-10-05")
    
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    
    cleaned_batch = []
    dropped_count = 0
    validation_failures = 0
    
    for row in batch:
        # Ensure date_time is correct object type
        dt = row.get("date_time")
        if isinstance(dt, str):
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            row["date_time"] = dt
            
        # 1. Apply Date Cropping Gate
        # Cleanse dataset to ensure dates fall between Jan 1, 2015 and Oct 22, 2023
        if dt < start_date or dt > end_date:
            dropped_count += 1
            continue
            
        # 2. Apply Defensive Data Quality Checks
        is_valid, errors = validate_row_schema(row, config)
        if not is_valid:
            validation_failures += 1
            err_msg = f"Data-quality failure on SiteID {row.get('site_id')} at {dt}: {', '.join(errors)}"
            logger.warning(err_msg)
            # Dispatch mock alerts for serious issues (e.g., negative values)
            if any("Negative" in err or "out of bounds" in err for err in errors):
                send_alert_notification(err_msg)
            # Skip corrupted rows to keep OLAP warehouse pristine
            continue
            
        # 3. Add row integrity MD5 checksum
        row["row_checksum"] = compute_row_checksum(row)
        
        cleaned_batch.append(row)
        
    if dropped_count > 0:
        logger.info(f"Cropped/dropped {dropped_count} records falling outside active dates ({start_str} to {end_str}).")
    if validation_failures > 0:
        logger.info(f"Filtered out {validation_failures} records due to validation failures.")
        
    return cleaned_batch

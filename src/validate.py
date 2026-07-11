import hashlib
import logging
from typing import Any, List, Dict, Tuple

logger = logging.getLogger("pipeline")

def compute_row_checksum(row_dict: Dict[str, Any]) -> str:
    """
    Computes an MD5 checksum hash for a row of data to guarantee transmission integrity.
    
    Excludes database-generated fields (like 'id') and the 'row_checksum' key itself to ensure
    reproducibility across raw, OLAP, and NoSQL databases. Keys are sorted alphabetically before
    hashing to prevent differences in hash values due to dictionary ordering.
    
    Parameters:
        row_dict (Dict[str, Any]): Dictionary containing the row data.
        
    Returns:
        str: 32-character hexadecimal MD5 checksum representing the row contents.
    """
    # Sort keys to ensure consistent hashing
    sorted_items = sorted([(str(k), str(v)) for k, v in row_dict.items() if k not in ['id', 'row_checksum']])
    hash_input = "".join([f"{k}:{v}" for k, v in sorted_items]).encode('utf-8')
    return hashlib.md5(hash_input).hexdigest()

def validate_row_schema(row: Dict[str, Any], config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates a single reading dictionary against schema and physical boundary limits.
    
    Performs critical null checks on required attributes, verifies numeric columns can
    be parsed into float values, and enforces validation boundaries configured in config.yaml
    (e.g., NOx concentration thresholds, ambient temperatures, and relative humidity).
    
    Parameters:
        row (Dict[str, Any]): Dictionary representing a single observation row.
        config (Dict[str, Any]): Global pipeline configuration dictionary.
        
    Returns:
        Tuple[bool, List[str]]: A tuple of (is_valid, validation_errors_list).
    """
    errors = []
    
    # 1. Null check on critical columns
    if row.get("date_time") is None:
        errors.append("Null Date/Time value")
    if row.get("site_id") is None:
        errors.append("Null SiteID value")
        
    # 2. Bound checks
    val_config = config.get("validation", {})
    
    # Validate NOx
    nox = row.get("nox")
    if nox is not None:
        if nox < val_config.get("acceptable_nox_min", 0.0):
            errors.append(f"Negative NOx value: {nox}")
        elif nox > val_config.get("acceptable_nox_max", 2000.0):
            errors.append(f"Extreme NOx value exceeding maximum: {nox}")
            
    # Validate Temperature
    temperature = row.get("temperature")
    if temperature is not None:
        if temperature < val_config.get("acceptable_temp_min", -20.0) or temperature > val_config.get("acceptable_temp_max", 45.0):
            errors.append(f"Temperature out of bounds: {temperature}°C")
            
    # Validate Humidity (RH)
    rh = row.get("rh")
    if rh is not None:
        if rh < val_config.get("acceptable_humidity_min", 0.0) or rh > val_config.get("acceptable_humidity_max", 100.0):
            errors.append(f"Relative humidity out of bounds: {rh}%")
            
    # 3. Check for specific numeric data type conformance
    for col in ["nox", "no2", "no", "pm10", "o3", "temperature", "nvpm10", "vpm10", "nvpm2_5", "pm2_5", "vpm2_5", "co", "rh", "air_pressure", "so2"]:
        val = row.get(col)
        if val is not None:
            try:
                float(val)
            except ValueError:
                errors.append(f"Column '{col}' value '{val}' cannot be parsed as float")

    return len(errors) == 0, errors

def send_alert_notification(message: str) -> None:
    """
    Mock alerting system to notify engineering team of validation failures.
    
    Logs critical errors to logs. In production, this handler executes webhooks to 
    notify external incident response platforms like PagerDuty or Slack.
    
    Parameters:
        message (str): Alert details to log and dispatch.
    """
    logger.critical(f"[ALERT NOTIFICATION DISPATCHED]: {message}")

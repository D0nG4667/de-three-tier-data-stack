import pytest
from datetime import datetime
from src.transform import transform_batch
from src.validate import compute_row_checksum, validate_row_schema

@pytest.fixture
def mock_config():
    return {
        "etl": {
            "crop_start_date": "2010-01-01",
            "crop_end_date": "2022-10-05"
        },
        "validation": {
            "acceptable_nox_min": 0.0,
            "acceptable_nox_max": 2000.0,
            "acceptable_temp_min": -20.0,
            "acceptable_temp_max": 45.0,
            "acceptable_humidity_min": 0.0,
            "acceptable_humidity_max": 100.0
        }
    }

def test_compute_row_checksum():
    row1 = {"site_id": 188, "val": 10.5, "temperature": 12.0}
    row2 = {"val": 10.5, "temperature": 12.0, "site_id": 188}  # Reordered keys
    row3 = {"site_id": 188, "val": 10.5, "temperature": 13.0}  # Different value
    
    hash1 = compute_row_checksum(row1)
    hash2 = compute_row_checksum(row2)
    hash3 = compute_row_checksum(row3)
    
    # Hash should be consistent regardless of key ordering
    assert hash1 == hash2
    # Different data should result in different hash
    assert hash1 != hash3
    # Check length
    assert len(hash1) == 32

def test_validate_row_schema_valid(mock_config):
    valid_row = {
        "date_time": datetime(2019, 10, 1, 8, 0, 0),
        "site_id": 188,
        "nox": 120.0,
        "temperature": 15.0,
        "rh": 50.0
    }
    is_valid, errors = validate_row_schema(valid_row, mock_config)
    assert is_valid is True
    assert len(errors) == 0

def test_validate_row_schema_invalid_nox(mock_config):
    invalid_row = {
        "date_time": datetime(2019, 10, 1, 8, 0, 0),
        "site_id": 188,
        "nox": -5.0,  # Negative NOx
        "temperature": 15.0,
        "rh": 50.0
    }
    is_valid, errors = validate_row_schema(invalid_row, mock_config)
    assert is_valid is False
    assert any("Negative NOx" in err for err in errors)

def test_validate_row_schema_invalid_temp(mock_config):
    invalid_row = {
        "date_time": datetime(2019, 10, 1, 8, 0, 0),
        "site_id": 188,
        "nox": 120.0,
        "temperature": 55.0,  # Temp out of bounds (>45)
        "rh": 50.0
    }
    is_valid, errors = validate_row_schema(invalid_row, mock_config)
    assert is_valid is False
    assert any("Temperature out of bounds" in err for err in errors)

def test_transform_batch_cropping(mock_config):
    batch = [
        # In range
        {"date_time": datetime(2018, 5, 1, 12, 0, 0), "site_id": 188, "nox": 50.0, "temperature": 15.0},
        # Out of range (too early)
        {"date_time": datetime(2009, 12, 31, 23, 0, 0), "site_id": 188, "nox": 50.0, "temperature": 15.0},
        # Out of range (too late)
        {"date_time": datetime(2022, 10, 6, 0, 0, 0), "site_id": 188, "nox": 50.0, "temperature": 15.0},
        # In range but invalid value (should be filtered out)
        {"date_time": datetime(2018, 5, 2, 12, 0, 0), "site_id": 188, "nox": -99.0, "temperature": 15.0}
    ]
    
    cleaned = transform_batch(batch, mock_config)
    
    # Only the first record is in range and valid
    assert len(cleaned) == 1
    assert cleaned[0]["date_time"] == datetime(2018, 5, 1, 12, 0, 0)
    assert "row_checksum" in cleaned[0]

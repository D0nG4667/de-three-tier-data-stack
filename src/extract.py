import logging
from typing import Any, Iterator, List, Dict

logger = logging.getLogger("pipeline")

def extract_raw_data_in_batches(conn: Any, chunk_size: int = 10000) -> Iterator[List[Dict[str, Any]]]:
    """
    Extracts raw readings from db-raw using efficient keyset cursor-based pagination.
    
    This function implements a memory-efficient generator that queries database chunks sequentially.
    By using 'id > last_id' filtering instead of OFFSET, it leverages the primary key index
    execution plan, keeping memory consumption low even on datasets exceeding millions of records.
    
    Parameters:
        conn (Any): Active psycopg2 connection object to the raw staging database.
        chunk_size (int): Number of records to retrieve per database query chunk.
        
    Yields:
        Iterator[List[Dict[str, Any]]]: Generator yielding list batches of record dictionaries.
    """
    last_id = 0
    total_extracted = 0
    
    # Define columns to fetch
    columns = [
        "id", "date_time", "site_id", "nox", "no2", "no", "pm10", "o3", "temperature",
        "nvpm10", "vpm10", "nvpm2_5", "pm2_5", "vpm2_5",
        "co", "rh", "air_pressure", "so2"
    ]
    col_str = ", ".join(columns)
    
    while True:
        with conn.cursor() as cur:
            # Using cursor-based filter (id > last_id) instead of OFFSET for optimal execution plan
            query = f"""
                SELECT {col_str} 
                FROM raw_readings 
                WHERE id > %s 
                ORDER BY id ASC 
                LIMIT %s;
            """
            cur.execute(query, (last_id, chunk_size))
            rows = cur.fetchall()
            
            if not rows:
                break
                
            batch_data = []
            for r in rows:
                row_dict = dict(zip(columns, r))
                batch_data.append(row_dict)
                last_id = r[0] # The first element is 'id'
                
            total_extracted += len(batch_data)
            logger.info(f"Extracted batch: {len(batch_data)} records (total extracted: {total_extracted})")
            yield batch_data
            
            if len(rows) < chunk_size:
                break

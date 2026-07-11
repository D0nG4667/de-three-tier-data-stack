import os
import sys
import csv
import dlt
import yaml
import math
import random
import psycopg2
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta
from src.downloader import download_and_extract_dataset

# Load configuration
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/app/config/config.yaml"))

def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# Hardcoded Station and Constituency Data (matching UWE specification)
CONSTITUENCIES = [
    {"id": 1, "name": "Bristol East", "mp_name": "Kerry McCarthy"},
    {"id": 2, "name": "Bristol Northwest", "mp_name": "Darren Jones"},
    {"id": 3, "name": "Bristol South", "mp_name": "Karin Smyth"},
    {"id": 4, "name": "Bristol West", "mp_name": "Thangam Debbonaire"}
]

STATIONS = [
    {"site_id": 188, "name": "AURN Bristol Centre", "lat": 51.4572041156, "lon": -2.58564914143, "const_id": 4, "date_start": None, "date_end": None, "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 203, "name": "Brislington Depot", "lat": 51.4417471805, "lon": -2.55995583219, "const_id": 1, "date_start": "2001-01-01 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 206, "name": "Rupert Street", "lat": 51.4554331987, "lon": -2.59626237324, "const_id": 4, "date_start": "2003-01-01 00:00:00", "date_end": "2015-12-31 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 209, "name": "IKEA M32", "lat": 51.4752847609, "lon": -2.56207998299, "const_id": 4, "date_start": "1998-01-10 00:00:00", "date_end": "2000-02-06 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 213, "name": "Old Market", "lat": 51.4560189999, "lon": -2.58348949026, "const_id": 1, "date_start": None, "date_end": None, "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 215, "name": "Parson Street School", "lat": 51.4326757073, "lon": -2.60495665668, "const_id": 3, "date_start": "2002-02-01 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 228, "name": "Temple Meads Station", "lat": 51.4488837041, "lon": -2.58447776241, "const_id": 4, "date_start": "2003-02-01 00:00:00", "date_end": "2003-10-27 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 270, "name": "Wells Road", "lat": 51.4278638885, "lon": -2.56374153310, "const_id": 1, "date_start": "2003-05-23 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 271, "name": "Trailer Portway P&R", "lat": 51.4899934596, "lon": -2.68877856929, "const_id": 2, "date_start": "2004-03-01 00:00:00", "date_end": "2009-03-01 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 375, "name": "Newfoundland Road Police Station", "lat": 51.4606738207, "lon": -2.58225341824, "const_id": 4, "date_start": "2005-01-01 00:00:00", "date_end": "2015-12-31 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 395, "name": "Shiner's Garage", "lat": 51.4577930324, "lon": -2.56271419977, "const_id": 1, "date_start": "2004-06-24 00:00:00", "date_end": "2013-01-04 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 452, "name": "AURN St Pauls", "lat": 51.4628294174, "lon": -2.58454081630, "const_id": 4, "date_start": "2006-06-15 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 447, "name": "Bath Road", "lat": 51.4425372726, "lon": -2.57137536073, "const_id": 4, "date_start": "2005-10-29 00:00:00", "date_end": "2013-01-04 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 459, "name": "Cheltenham Road \\ Station Road", "lat": 51.4689385901, "lon": -2.5927241667, "const_id": 4, "date_start": "2008-06-25 00:00:00", "date_end": "2011-12-31 00:00:00", "is_current": False, "instrument_type": "Continuous (Reference)"},
    {"site_id": 463, "name": "Fishponds Road", "lat": 51.4780449717, "lon": -2.53523027454, "const_id": 1, "date_start": "2009-03-13 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 481, "name": "CREATE Centre Roof", "lat": 51.447213417, "lon": -2.62247405516, "const_id": 4, "date_start": "2003-05-23 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 500, "name": "Temple Way", "lat": 51.4579497132, "lon": -2.58398909028, "const_id": 4, "date_start": "2017-08-01 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 501, "name": "Colston Avenue", "lat": 51.4552693827, "lon": -2.59664882855, "const_id": 4, "date_start": "2018-11-30 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"},
    {"site_id": 672, "name": "Marlborough Street", "lat": 51.4591419717, "lon": -2.59543271836, "const_id": 4, "date_start": "2021-07-01 00:00:00", "date_end": None, "is_current": True, "instrument_type": "Continuous (Reference)"}
]

def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("RAW_DB_HOST", "db-raw"),
        port=os.getenv("RAW_DB_PORT", "5432"),
        user=os.getenv("RAW_DB_USER", "postgres"),
        password=os.getenv("RAW_DB_PASSWORD", "postgres_raw_password"),
        database=os.getenv("RAW_DB_NAME", "bristol_raw")
    )

def get_connection_string() -> str:
    host = os.getenv("RAW_DB_HOST", "db-raw")
    port = os.getenv("RAW_DB_PORT", "5432")
    user = os.getenv("RAW_DB_USER", "postgres")
    password = os.getenv("RAW_DB_PASSWORD", "postgres_raw_password")
    database = os.getenv("RAW_DB_NAME", "bristol_raw")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

def setup_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        print("Setting up schemas in raw database...")
        cur.execute("""
            DROP TABLE IF EXISTS raw_readings CASCADE;
            DROP TABLE IF EXISTS raw_stations CASCADE;
            DROP TABLE IF EXISTS raw_constituencies CASCADE;

            CREATE TABLE raw_constituencies (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                mp_name VARCHAR(100) NOT NULL
            );

            CREATE TABLE raw_stations (
                site_id INT PRIMARY KEY,
                name VARCHAR(150) NOT NULL,
                latitude DECIMAL(12,9) NOT NULL,
                longitude DECIMAL(12,9) NOT NULL,
                constituency_id INT REFERENCES raw_constituencies(id),
                date_start TIMESTAMP,
                date_end TIMESTAMP,
                is_current BOOLEAN,
                instrument_type VARCHAR(100)
            );

            CREATE TABLE raw_readings (
                id SERIAL PRIMARY KEY,
                date_time TIMESTAMP NOT NULL,
                site_id INT REFERENCES raw_stations(site_id),
                nox REAL,
                no2 REAL,
                no REAL,
                pm10 REAL,
                o3 REAL,
                temperature REAL,
                nvpm10 REAL,
                vpm10 REAL,
                nvpm2_5 REAL,
                pm2_5 REAL,
                vpm2_5 REAL,
                co REAL,
                rh REAL,
                air_pressure REAL,
                so2 REAL,
                _dlt_load_id VARCHAR(32),
                _dlt_id VARCHAR(32)
            );
        """)
        conn.commit()
        print("Raw database tables created.")

def seed_static_tables(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        print("Seeding static tables...")
        for const in CONSTITUENCIES:
            cur.execute(
                "INSERT INTO raw_constituencies (id, name, mp_name) VALUES (%s, %s, %s);",
                (const["id"], const["name"], const["mp_name"])
            )
        for stat in STATIONS:
            cur.execute(
                """
                INSERT INTO raw_stations (
                    site_id, name, latitude, longitude, constituency_id,
                    date_start, date_end, is_current, instrument_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    stat["site_id"], stat["name"], stat["lat"], stat["lon"], stat["const_id"],
                    stat["date_start"], stat["date_end"], stat["is_current"], stat["instrument_type"]
                )
            )
        conn.commit()
        print("Constituencies and Stations seeded.")

def generate_sensor_readings(start_date: datetime, end_date: datetime) -> List[Tuple[Any, ...]]:
    """
    Generates realistic sensor readings using traffic patterns, day/night cycles,
    seasonal profiles, and random faults.
    """
    delta = end_date - start_date
    total_hours = int(delta.total_seconds() / 3600)
    print(f"Generating data from {start_date} to {end_date} ({total_hours} hourly readings per station)")

    random.seed(42)
    batch_records = []
    current_time = start_date
    
    for h in range(total_hours):
        hour_val = current_time.hour
        day_of_week = current_time.weekday()
        month_val = current_time.month

        base_temp = 10.0 + 8.0 * math.sin(2 * math.pi * (month_val - 4) / 12.0)
        daily_temp_var = 5.0 * math.sin(2 * math.pi * (hour_val - 8) / 24.0)
        temp = base_temp + daily_temp_var + random.uniform(-2, 2)

        rh = max(10.0, min(100.0, 80.0 - 25.0 * math.sin(2 * math.pi * (hour_val - 8) / 24.0) + random.uniform(-5, 5)))
        pressure = 1013.25 + 15.0 * math.sin(2 * math.pi * h / 200.0) + random.uniform(-3, 3)

        for station in STATIONS:
            site_id = station["site_id"]
            
            traffic_factor = 1.0
            if hour_val in [7, 8, 9]:
                traffic_factor = random.uniform(2.5, 4.5)
            elif hour_val in [16, 17, 18, 19]:
                traffic_factor = random.uniform(2.0, 4.0)
            elif hour_val in [0, 1, 2, 3, 4]:
                traffic_factor = random.uniform(0.2, 0.6)
            
            if day_of_week >= 5:
                traffic_factor *= 0.6

            station_factor = 1.2 if site_id in [188, 206, 228, 500] else 0.8

            base_nox = random.uniform(15, 45) * traffic_factor * station_factor
            no2 = base_nox * random.uniform(0.4, 0.6)
            no = (base_nox - no2) * 1.5
            nox = no2 + no

            pm10 = random.uniform(5, 15) * traffic_factor * station_factor
            pm2_5 = pm10 * random.uniform(0.5, 0.7)
            
            vpm10 = pm10 * random.uniform(0.1, 0.3)
            nvpm10 = pm10 - vpm10
            vpm2_5 = pm2_5 * random.uniform(0.1, 0.3)
            nvpm2_5 = pm2_5 - vpm2_5

            o3 = max(0.0, (20.0 + 1.5 * temp) * (1.0 + 0.5 * math.sin(2 * math.pi * (hour_val - 12) / 24.0)) + random.uniform(-5, 5))
            co = random.uniform(0.1, 0.8) * traffic_factor
            so2 = random.uniform(0.5, 4.0) * station_factor

            # Chaos / Anomaly Injections
            rand_roll = random.random()
            if rand_roll < 0.0005:
                nox, no2, no, pm10, o3, co, so2 = [None] * 7
            elif rand_roll < 0.001:
                nox = -999.0
                no2 = -999.0
            elif rand_roll < 0.0015:
                nox = 3500.0
                no2 = 1800.0
            elif rand_roll < 0.002:
                temp = -99.0

            batch_records.append((
                current_time, site_id, nox, no2, no, pm10, o3, temp,
                nvpm10, vpm10, nvpm2_5, pm2_5, vpm2_5,
                co, rh, pressure, so2
            ))
            
        current_time += timedelta(hours=1)
        
    return batch_records

def ingest_csv_dataset(
    conn: psycopg2.extensions.connection,
    connection_string: str,
    csv_path: Path
) -> None:
    """
    Reads the original UWE Bristol CSV dataset using a memory-safe pure Python DictReader stream
    and loads it into db-raw via dlt using native PostgreSQL COPY.
    """
    print(f"Ingesting raw CSV dataset from {csv_path} using dlt...")
    
    with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
    
    separator = ';' if ';' in first_line else ','
    print(f"Detected CSV separator: '{separator}'")

    # Column mapping (maps CSV headers to database attributes)
    col_mapping = {
        'date time': 'date_time',
        'siteid': 'site_id',
        'nox': 'nox',
        'no2': 'no2',
        'no': 'no',
        'pm10': 'pm10',
        'o3': 'o3',
        'temperature': 'temperature',
        'nvpm10': 'nvpm10',
        'vpm10': 'vpm10',
        'nvpm2.5': 'nvpm2_5',
        'pm2.5': 'pm2_5',
        'vpm2.5': 'vpm2_5',
        'co': 'co',
        'rh': 'rh',
        'air pressure': 'air_pressure',
        'so2': 'so2'
    }

    db_cols = list(dict.fromkeys(col_mapping.values()))
    valid_station_ids = {s["site_id"] for s in STATIONS}

    # Truncate raw_readings to prevent duplicates on re-runs while keeping constraints
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_readings;")
        conn.commit()

    @dlt.resource(table_name="raw_readings", write_disposition="append")
    def csv_data_resource():
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f, delimiter=separator)
            for row in reader:
                cleaned_row = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    clean_k = k.strip().lower()
                    if clean_k in col_mapping:
                        db_col = col_mapping[clean_k]
                        cleaned_row[db_col] = None if (v == "" or v is None or v == "NaN" or v == "null") else v
                
                # Check site_id
                site_id_raw = cleaned_row.get("site_id")
                if not site_id_raw:
                    continue
                try:
                    site_id = int(float(site_id_raw))
                    if site_id not in valid_station_ids:
                        continue
                    cleaned_row["site_id"] = site_id
                except ValueError:
                    continue

                # Check date_time
                dt_raw = cleaned_row.get("date_time")
                if not dt_raw:
                    continue
                dt_obj = None
                try:
                    dt_obj = datetime.fromisoformat(dt_raw)
                except ValueError:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S%z"):
                        try:
                            dt_obj = datetime.strptime(dt_raw, fmt)
                            break
                        except ValueError:
                            continue
                if dt_obj is None:
                    continue
                cleaned_row["date_time"] = dt_obj

                # Numeric casting
                for col in db_cols:
                    if col not in ("date_time", "site_id"):
                        val = cleaned_row.get(col)
                        if val is not None:
                            try:
                                cleaned_row[col] = float(val)
                            except ValueError:
                                cleaned_row[col] = None
                        else:
                            cleaned_row[col] = None

                yield cleaned_row

    pipeline = dlt.pipeline(
        pipeline_name="uwe_bristol_csv_ingest_v2",
        destination="postgres",
        dataset_name="public"
    )
    
    load_info = pipeline.run(
        csv_data_resource(),
        credentials=connection_string
    )
    print(load_info)
    print("CSV dataset ingestion via dlt complete.")

def load_simulated_readings(
    conn: psycopg2.extensions.connection,
    connection_string: str,
    records: List[Tuple[Any, ...]]
) -> None:
    """
    Loads simulated time-series readings into db-raw via dlt using native PostgreSQL COPY.
    """
    print(f"Loading {len(records)} simulated readings into db-raw using dlt...")
    
    # Truncate raw_readings to prevent duplicates on re-runs while keeping constraints
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_readings;")
        conn.commit()

    @dlt.resource(table_name="raw_readings", write_disposition="append")
    def simulated_data_resource():
        keys = [
            "date_time", "site_id", "nox", "no2", "no", "pm10", "o3", "temperature",
            "nvpm10", "vpm10", "nvpm2_5", "pm2_5", "vpm2_5", "co", "rh", "air_pressure", "so2"
        ]
        for r in records:
            yield dict(zip(keys, r))

    pipeline = dlt.pipeline(
        pipeline_name="uwe_bristol_sim_ingest_v2",
        destination="postgres",
        dataset_name="public"
    )
    
    load_info = pipeline.run(
        simulated_data_resource(),
        credentials=connection_string
    )
    print(load_info)
    print("Simulated readings loading via dlt complete.")

def main() -> None:
    config = load_config()
    ingest_conf = config.get("ingestion", {})
    mode = ingest_conf.get("mode", "simulate")

    print(f"Starting Seeding Engine. Ingestion Mode: {mode.upper()}")
    
    conn = get_connection()
    connection_string = get_connection_string()
    try:
        setup_schema(conn)
        seed_static_tables(conn)

        if mode == "csv":
            csv_path = Path(ingest_conf.get("csv_path", "/app/data/air_quality_data_continuous.csv"))
            
            # Download and extract if missing and allowed
            if not csv_path.exists():
                if ingest_conf.get("download_on_missing", True):
                    download_url = ingest_conf.get("download_url")
                    dest_dir = csv_path.parent
                    csv_path = download_and_extract_dataset(download_url, dest_dir, csv_name=csv_path.name)
                else:
                    raise FileNotFoundError(f"Original dataset CSV not found at {csv_path} and download_on_missing is disabled.")
                    
            ingest_csv_dataset(conn, connection_string, csv_path)
        else:
            # Simulation mode
            generate_full = os.getenv("GENERATE_FULL_DATASET", "false").lower() == "true"
            gen_conf = config.get("generator", {})
            start_yr = gen_conf.get("start_year", 2010)
            end_yr = gen_conf.get("end_year", 2022)
            
            if generate_full:
                start_date = datetime(start_yr, 1, 1, 0, 0, 0)
                end_date = datetime(end_yr, 10, 5, 23, 0, 0)
            else:
                start_date = datetime(2018, 6, 1, 0, 0, 0)
                end_date = datetime(2020, 6, 1, 0, 0, 0)
                print("Using rapid development mode (June 2018 - June 2020 + Oct 2019 slice).")

            records = generate_sensor_readings(start_date, end_date)
            
            if not generate_full:
                print("Injecting 2019 sample data for query completeness...")
                slice_2019_start = datetime(2019, 10, 1, 0, 0, 0)
                slice_2019_end = datetime(2019, 10, 30, 23, 0, 0)
                records += generate_sensor_readings(slice_2019_start, slice_2019_end)

            load_simulated_readings(conn, connection_string, records)
                
    except Exception as e:
        print(f"\nError occurred during seeding: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

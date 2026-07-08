-- UFCFLR-15-M Data Management Fundamentals
-- Modelling & Mapping Bristol Air Quality Data
-- Task 2: Forward Engineered Database Schema (PostgreSQL/MySQL Compatible)

DROP TABLE IF EXISTS readings CASCADE;
DROP TABLE IF EXISTS stations CASCADE;
DROP TABLE IF EXISTS constituencies CASCADE;

-- 1. Constituencies Table
CREATE TABLE constituencies (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    mp_name VARCHAR(100) NOT NULL
);

-- 2. Stations (Monitors) Table
CREATE TABLE stations (
    site_id INT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    latitude DECIMAL(12,9) NOT NULL,
    longitude DECIMAL(12,9) NOT NULL,
    constituency_id INT,
    date_start TIMESTAMP,
    date_end TIMESTAMP,
    is_current BOOLEAN,
    instrument_type VARCHAR(100),
    FOREIGN KEY (constituency_id) REFERENCES constituencies(id) ON DELETE SET NULL
);

-- 3. Readings (Observations) Table
CREATE TABLE readings (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    site_id INT NOT NULL,
    nox REAL,
    no2 REAL,
    no REAL,
    pm10 REAL,
    o3 REAL,
    temp REAL,
    nvpm10 REAL,
    vpm10 REAL,
    nvpm2_5 REAL,
    pm2_5 REAL,
    vpm2_5 REAL,
    co REAL,
    rh REAL,
    pressure REAL,
    so2 REAL,
    row_checksum VARCHAR(32) NOT NULL,
    FOREIGN KEY (site_id) REFERENCES stations(site_id) ON DELETE CASCADE
);

-- Performance Optimization Indexes
CREATE INDEX idx_readings_date_time ON readings(date_time);
CREATE INDEX idx_readings_site_id ON readings(site_id);
CREATE INDEX idx_readings_composite ON readings(site_id, date_time);

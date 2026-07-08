-- UFCFLR-15-M Data Management Fundamentals
-- Modelling & Mapping Bristol Air Quality Data
-- Task 5(i): Highest recorded NOx value in the year 2019

SELECT 
    r.date_time,
    s.name AS station_name,
    r.nox AS highest_nox
FROM readings r
JOIN stations s ON r.site_id = s.site_id
WHERE r.date_time >= '2019-01-01 00:00:00'
  AND r.date_time <= '2019-12-31 23:00:00'
  AND r.nox IS NOT NULL
  AND r.nox != 'NaN'::real
ORDER BY r.nox DESC
LIMIT 1;

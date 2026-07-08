-- UFCFLR-15-M Data Management Fundamentals
-- Modelling & Mapping Bristol Air Quality Data
-- Task 5(iii): Mean PM2.5 and VPM2.5 values at or near 08:00 hours for all stations in the years 2010 to 2019

SELECT 
    s.name AS station_name,
    AVG(CASE WHEN r.pm2_5 = 'NaN'::real THEN NULL ELSE r.pm2_5 END) AS mean_pm2_5,
    AVG(CASE WHEN r.vpm2_5 = 'NaN'::real THEN NULL ELSE r.vpm2_5 END) AS mean_vpm2_5
FROM readings r
JOIN stations s ON r.site_id = s.site_id
-- Extracting hour 8 represents observations taken at 08:00 hours
WHERE EXTRACT(HOUR FROM r.date_time) = 8
  AND EXTRACT(YEAR FROM r.date_time) BETWEEN 2010 AND 2019
GROUP BY s.name
ORDER BY s.name;

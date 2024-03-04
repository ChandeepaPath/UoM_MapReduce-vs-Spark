-- Script: year_wise_security_delay.hql

-- Create an external table to load the data from your CSV file (if not already created)
CREATE EXTERNAL TABLE IF NOT EXISTS delayed_flights (
    Year INT,
    Month INT,
    DayOfMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://delayed-flights-assignment/Hive/hive-dataset/preprocessed_delayed_flights.csv';

-- Query to get year-wise security delay from 2003 to 2010
SELECT Year, AVG(SecurityDelay) AS AVGSecurityDelay
FROM delayed_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;

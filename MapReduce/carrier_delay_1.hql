-- Create Table Schema with columns
CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum STRING,
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
    Cancelled BOOLEAN,
    CancellationCode STRING,
    Diverted BOOLEAN,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    LateAircraftDelay INT,
    SecurityDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Load the data from csv to the Hive table
LOAD DATA INPATH '${INPUT}' INTO TABLE delay_flights;

-- Run desired query & write output to the given location
-- write query start & end times for each query

-- run & write outputs of each query with execution time query
INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/timestamps/start_time' 
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:carrier_delay_query_results = SELECT Year, AVG(CarrierDelay) AS Avg_Carrier_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/timestamps/end_time' 
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/results' 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    ${hivevar:carrier_delay_query_results};
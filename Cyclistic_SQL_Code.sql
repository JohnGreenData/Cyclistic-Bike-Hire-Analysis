--_____CLEANING

--clear the table for code to be re-run from scratch.
DROP TABLE IF EXISTS Raw_Data_2024_AllMonths;
DROP TABLE IF EXISTS Cleaned_Data_2024_AllMonths;

--Combine all records into one table; excluding duplicate records in May / June.
CREATE TABLE Raw_Data_2024_AllMonths AS
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202401-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202402-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202403-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202404-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202405-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202406-divvy-tripdata.csv')
  WHERE ride_id NOT IN (
    SELECT ride_id
    FROM (SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202405-divvy-tripdata.csv') UNION ALL SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202406-divvy-tripdata.csv'))
    GROUP BY ride_id
    HAVING COUNT(ride_id) > 1
    )
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202407-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202408-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202409-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202410-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202411-divvy-tripdata.csv')
  UNION ALL
  SELECT * FROM read_csv_auto('C:/Users/johng/Desktop/Data/Cyclistic_Add-on/Cyclistic_existing_project/Data/Raw_Data/202412-divvy-tripdata.csv');

--duplicate table for safety
CREATE TABLE Cleaned_Data_2024_AllMonths AS SELECT * FROM Raw_Data_2024_AllMonths;

--note the number of records at this stage
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths;

--create and populate ride length column
ALTER TABLE Cleaned_Data_2024_AllMonths
ADD COLUMN ride_length_in_mins DOUBLE;

UPDATE Cleaned_Data_2024_AllMonths
SET ride_length_in_mins = 
  EXTRACT(YEAR FROM ended_at - started_at) * 525600 +
  EXTRACT(MONTH FROM ended_at - started_at) * 43800 +
  EXTRACT(DAY FROM ended_at - started_at) * 1440 +
  EXTRACT(HOUR FROM ended_at - started_at) * 60 +
  EXTRACT(MINUTE FROM ended_at - started_at) +
  EXTRACT(SECOND FROM ended_at - started_at) / 60
WHERE ride_id = ride_id;

--create and populate day of week column
ALTER TABLE Cleaned_Data_2024_AllMonths
ADD COLUMN day_of_week INTEGER;

UPDATE Cleaned_Data_2024_AllMonths
SET day_of_week = EXTRACT(ISODOW FROM started_at);

--create and populate a weekday / weekend column
ALTER TABLE Cleaned_Data_2024_AllMonths
ADD COLUMN weekday_weekend STRING;

UPDATE Cleaned_Data_2024_AllMonths
SET weekday_weekend = 'weekday'
WHERE day_of_week BETWEEN 1 AND 5;

UPDATE Cleaned_Data_2024_AllMonths
SET weekday_weekend = 'weekend'
WHERE day_of_week BETWEEN 6 AND 7;

--create and populate an hour of the day column
ALTER TABLE Cleaned_Data_2024_AllMonths
ADD COLUMN hour_of_day INTEGER;

UPDATE Cleaned_Data_2024_AllMonths
SET hour_of_day = EXTRACT(HOUR FROM started_at);

--note how many records contain durations of 0 or less.
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins <= 0;

--delete records where the ride duration is 0 or less. (These records were due to maintenance and can be ignored.)
DELETE FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins <= 0;

--remove ride-duration outliers that will skew the averages by large amounts.
--first calculate a good upper outlier threshold - view upper end percentiles of data distribution.
SELECT
  QUANTILE_CONT(ride_length_in_mins, 0.90) AS p90,
  QUANTILE_CONT(ride_length_in_mins, 0.95) AS p95,
  QUANTILE_CONT(ride_length_in_mins, 0.99) AS p99,
  QUANTILE_CONT(ride_length_in_mins, 0.995) AS p99_5,
  QUANTILE_CONT(ride_length_in_mins, 0.996) AS p99_6,
  QUANTILE_CONT(ride_length_in_mins, 0.999) AS p99_9,
  MAX(ride_length_in_mins) AS max_val
FROM Cleaned_Data_2024_AllMonths;

--up to 120 mins seems sensible from a business plausibility perspective.  Let's see exactly how much of the data this covers:
SELECT
  COUNT(*) AS zero_to_120_mins,
  (SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE ride_length_in_mins > 120) AS over_120_mins,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths) AS percent_within_0_to_120_mins
FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins BETWEEN 0 AND 120;
--Having examined percentiles I have selected an upper threshold of 120 mins, based on the 99th+ percentile; and from a business plausibility perspective, a two hour ride being a plausible occurance for those on a leisure ride.  This balances statistical integrity and business plausibility.

--let's determine a good lower outlier threshold.  From a business plausibility perspective, 30 seconds seems like a sensible lower outlier threshold.
--how many records does this segment make up?
SELECT
  COUNT(*) AS under_30_sec,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths) AS pct
FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins < 0.5;
--this covers 1.6% of the data.  So is not negligible.

--we have already dropped journeys of 0 mins or less, as these are due to maintenance.
--let's look at distribution of rides up to 30 seconds:
SELECT
  ROUND(ride_length_in_mins,1) AS duration,
  COUNT(*) AS rides
FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins < 0.5
GROUP BY duration
ORDER BY duration;
--the distribution is relatively smooth, so there are no logging artefacts.
--despite the above, from a business plausibility perspective, I have decided to eliminate rides under 30 seconds from the analysis.  This is because by the time a customer has hired a bike and ridden a very short journey to save a slightly longer walk and then off-hired the bike this is the lowest plausible length of ride.  This is not due to issues with the data.  I'm classing rides under 30 seconds as abandoned trips and false starts, mistaken hires etc.

--a final sanity check comparison - stats with and without outliers removed
SELECT
  'All' as outliers,
  member_casual,
  COUNT(*) AS rides,
  ROUND(AVG(ride_length_in_mins),2) AS avg_mins,
  ROUND(MEDIAN(ride_length_in_mins),2) AS median_mins,
  ROUND(QUANTILE_CONT(ride_length_in_mins, 0.25),2) AS p25_mins,
  ROUND(QUANTILE_CONT(ride_length_in_mins, 0.75),2) AS p75_mins,
  ROUND(MAX(ride_length_in_mins),2) AS max_mins
FROM Cleaned_Data_2024_AllMonths
GROUP BY member_casual
UNION ALL
SELECT
  '0.5-120 min only' as outliers,
  member_casual,
  COUNT(*) AS rides,
  ROUND(AVG(ride_length_in_mins),2) AS avg_mins,
  ROUND(MEDIAN(ride_length_in_mins),2) AS median_mins,
  ROUND(QUANTILE_CONT(ride_length_in_mins, 0.25),2) AS p25_mins,
  ROUND(QUANTILE_CONT(ride_length_in_mins, 0.75),2) AS p75_mins,
  ROUND(MAX(ride_length_in_mins),2) AS max_mins
FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins BETWEEN 0.5 AND 120
GROUP BY member_casual
ORDER BY member_casual, outliers;
--with outliers removed this has a large impact on average length of journey, but a relatively small impact on median and percentiles, which is a good sign that 0.5-120mins is a strong outlier threshold.

--count the number of outliers to be removed for cross checking.
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins NOT BETWEEN 0.5 AND 120;

--go ahead and remove these outliers...
DELETE FROM Cleaned_Data_2024_AllMonths
WHERE ride_length_in_mins NOT BETWEEN 0.5 AND 120;

--are there any records with null start_station_name?
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE start_station_name IS NULL;
-- yes.  I have checked the records affected externally, these records have no start_station_name externally, so this is not an import error.
-- but do they have a start_station_id instead?  Ie will this return the same number of records.
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE start_station_name IS NULL AND start_station_id IS NULL;
--they are the same, so where the station name is missing, so is the station ID.  So check that they have lat and lng data instead.
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE start_station_name IS NULL AND (start_lat IS NULL OR  start_lng IS NULL);
--yes, records with NULL start_station_name / id, do have start_lat / start_lng instead.  So it's safe to assume that these records are reliable and refer to start locations that are away from set stations; so I will not drop these records.

--same tests for end locations.
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE end_station_name IS NULL;
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE end_station_name IS NULL AND end_station_id IS NULL;
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths WHERE end_station_name IS NULL AND (end_lat IS NULL OR end_lng IS NULL);
--situation is the same for end locations, so we will not drop these records either.

--check for NULLs in other columns.
SELECT COUNT(*)
FROM Cleaned_Data_2024_AllMonths
WHERE 
	ride_id IS NULL OR
	rideable_type IS NULL OR
	started_at IS NULL OR
	ended_at IS NULL OR
	start_lat IS NULL OR
	start_lng IS NULL OR
	end_lat IS NULL OR
	end_lng IS NULL OR
	member_casual IS NULL;
-- no nulls in other columns, no action required.

--confirm final number of records in cleaned dataset
SELECT COUNT(*) FROM Cleaned_Data_2024_AllMonths;



--____ANALYSIS

--initial exploratory analysis
--1: member_casual
CREATE TABLE Agg01_membercasual AS
	SELECT
		member_casual,
  		ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins,
  		COUNT(*) AS counter, 
  		ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
	FROM Cleaned_Data_2024_AllMonths
	GROUP BY member_casual;

--2: rideable type
CREATE TABLE Agg02_rideabletype AS
  SELECT 
  	rideable_type, 
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, 
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY rideable_type;

--3: day of week
CREATE TABLE Agg03_dayofweek AS
  SELECT 
  	day_of_week, 
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins,
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY day_of_week
  ORDER BY day_of_week;

--4: month
CREATE TABLE Agg04_month AS
  SELECT 
  	EXTRACT(MONTH FROM started_at) AS month, 
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins,
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY month
  ORDER BY month;

--5: member_casual + rideable type
CREATE TABLE Agg05_membercasual_rideabletype AS
  SELECT 
  	member_casual, 
  	rideable_type, 
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, 
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY 
  	rideable_type, 
  	member_casual
  ORDER BY 
	rideable_type, 
	member_casual;

--6: member_casual + day of week
CREATE TABLE Agg06_membercasual_dayofweek AS
  SELECT 
  	member_casual,
  	day_of_week,
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, 
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY 
  	day_of_week, 
  	member_casual
  ORDER BY 
	day_of_week, 
	member_casual;

--7: member_casual + month
CREATE TABLE Agg07_membercasual_month AS
  SELECT 
  	member_casual, 
  	EXTRACT(MONTH FROM started_at) AS month, 
  	ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins,
  	COUNT(*) AS counter, 
  	ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM Cleaned_Data_2024_AllMonths
  GROUP BY 
  	month, 
  	member_casual
  ORDER BY 
	month, 
	member_casual;


--deeper behavioural analysis
--8: Weekday vs Weekend behavioural summary, incl: total rides, rides normalised per day (5 weekdays / 2 weekend days), % share of rides within each user type, average + median ride length
CREATE TABLE Agg08_membercasual_weekday_weekend AS
	SELECT
		member_casual,
		weekday_weekend,
		COUNT(*) AS count,
		
		ROUND(
			COUNT(*) / CASE
				WHEN weekday_weekend = 'weekday' THEN 5
				WHEN weekday_weekend = 'weekend' THEN 2
			END
		, 0) AS rides_per_day,
  		ROUND(
  			COUNT(*) * 100.0 / SUM (COUNT(*)) OVER (PARTITION BY member_casual)
  		, 2) AS pct_of_rides, 
  		ROUND(AVG(ride_length_in_mins), 2) AS avg_length_mins,
  		ROUND(MEDIAN(ride_length_in_mins), 2) AS median_length_mins,
  		ROUND(SUM(ride_length_in_mins), 2) AS total_length_mins
	FROM Cleaned_Data_2024_AllMonths
	GROUP BY 
		member_casual, 
		weekday_weekend
	ORDER BY
		member_casual,
		weekday_weekend;
--When normalised per day, members exhibit higher weekday ride frequency, consistent with commuter-style usage patterns. Casuals show higher weekend ride frequency and longer average ride durations on weekends, suggesting leisure-oriented behaviour. However, both groups demonstrate meaningful activity across the full week, indicating blended usage patterns rather than purely commuter or leisure segmentation.

--9: Do casuals and members behave differently depending hour of the day?
CREATE TABLE Agg09_membercasual_hour AS
	SELECT
		member_casual,
  		hour_of_day,
  		COUNT(*) AS rides,
  		ROUND(AVG(ride_length_in_mins), 2) AS avg_length_mins,
  		ROUND(SUM(ride_length_in_mins)) AS total_ride_len_mins,
  		ROUND(MEDIAN(ride_length_in_mins), 2) AS median_length_mins,
  		-- % of each user type's rides in each hour
  		ROUND(
  			COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY member_casual)
  		, 2) AS pct_of_rides
  	FROM Cleaned_Data_2024_AllMonths
  	GROUP BY 
  		member_casual,
  		hour_of_day
	ORDER BY 
		hour_of_day, 
		member_casual;
--Members show clear commuter peaks (8am and 5–6pm), while casual users ride more in the afternoon and for longer journeys (around 6 minutes longer on average), indicating potentially more leisure-focused use.

--10: Do members tend to use stations less than casuals?  As discussed during the cleaning - nulls in start_station_name/id or end_station_name/id represents journeys that were started or ended at a non-station location.  How do these split between casuals and members?
CREATE TABLE Agg10_membercasual_pct_from_station AS
	SELECT
		member_casual,
		COUNT(*) AS total,
		COUNT(start_station_name) AS with_start_station,
		COUNT(end_station_name) AS with_end_station,
		(with_start_station / total) * 100 AS percentage_with_start_station,
		(with_end_station / total) * 100 AS percentage_with_end_station
	FROM Cleaned_Data_2024_AllMonths
	GROUP BY member_casual;
--there is no huge % difference between members and casuals on this metric - 80-83%, so no real insight to gain here.

--11: How widely are journeys made to/from different stations by each user type?   Measuring the distinct starting points for each user type.
CREATE TABLE Agg11_membercasual_diversity_of_start_location AS
	SELECT
		member_casual,
		COUNT(DISTINCT start_station_name) AS unique_start_locations,
		COUNT(DISTINCT end_station_name) AS unique_end_locations
	FROM Cleaned_Data_2024_AllMonths
	GROUP BY member_casual
	ORDER BY member_casual;
--Members starting and ending locations are circa 5-7% more geographically distributed usage (e.g. commuting many office locations), whereas casual riders may be slightly more concentrated around specific popular areas.

--12: Top 10 starting station locations for members.  This excludes records with null start_station_names.
CREATE TABLE Agg12_member_top10_start_locations AS
	SELECT
		member_casual,
		start_station_name,
		COUNT(*) AS rides,
	FROM Cleaned_Data_2024_AllMonths
	WHERE 
		member_casual = 'member' AND
		start_station_name IS NOT NULL
	GROUP BY 
		member_casual,
		start_station_name
	ORDER BY rides DESC
	LIMIT 10;

--13: same for casuals
CREATE TABLE Agg13_casual_top10_start_locations AS
	SELECT
		member_casual,
		start_station_name,
		COUNT(*) AS rides
	FROM Cleaned_Data_2024_AllMonths
	WHERE 
		member_casual = 'casual' AND 
		start_station_name IS NOT NULL
	GROUP BY member_casual, start_station_name
	ORDER BY rides DESC
	LIMIT 10;
--Casual riders frequently start rides at locations such as parks and entertainment areas, while member rides are more common at office locations and transport stations. This pattern is consistent with leisure-oriented use for casual riders and more routine, potentially commuter-style usage for members, although trip purpose cannot be directly confirmed from the data.


--With more time, I would look to investigate trip distance (not just duration) for casuals, to calculate average speeds of travel, compared to members, to see if that supports existing theories.


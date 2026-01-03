--_____CLEANING

--Combine all records into one table; excluding duplicate records in May / June.
CREATE TABLE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths AS
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_01
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_02
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_03
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_04
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_05
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_06
  WHERE ride_id NOT IN (
    SELECT ride_id
    FROM (SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_05 UNION ALL SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_06)
    GROUP BY ride_id
    HAVING COUNT(ride_id) > 1
    )
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_07
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_08
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_09
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_10
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_11
  UNION ALL
  SELECT * FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_12;

--create ride length column, then populate the column
ALTER TABLE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
ADD COLUMN ride_length_in_mins FLOAT64;

UPDATE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
SET ride_length_in_mins = 
  EXTRACT(YEAR FROM ended_at - started_at) * 525600 +
  EXTRACT(MONTH FROM ended_at - started_at) * 43800 +
  EXTRACT(DAY FROM ended_at - started_at) * 1440 +
  EXTRACT(HOUR FROM ended_at - started_at) * 60 +
  EXTRACT(MINUTE FROM ended_at - started_at) +
  EXTRACT(SECOND FROM ended_at - started_at) / 60
WHERE ride_id = ride_id;

--create day of week column, then populate the column
ALTER TABLE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
ADD COLUMN day_of_week INTEGER;

UPDATE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
SET day_of_week = CAST (FORMAT_DATE('%w', started_at) AS INT)
WHERE ride_id = ride_id;

--in the day of the week column, change 0s to 7s for easier visualisations
UPDATE secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
SET day_of_week = 7
WHERE day_of_week = 0;

--delete records where the ride duration is 0 or less. (These records were due to maintenance and can be ignored.)
DELETE FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
WHERE ride_length_in_mins <= 0;


--____ANALYSIS

--notable existing columns:  rideable_type, member_casual, day_of_week, ride_length_in_mins

--_____singular aggregations
--aggregation 1: member_casual vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis01_membercasual_vs AS
  SELECT member_casual, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY member_casual;

--aggregation 2: rideable_type vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis02_rideabletype_vs AS
  SELECT rideable_type, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY rideable_type;

--aggregation 3: day_of_week vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis03_dayofweek_vs AS
  SELECT day_of_week, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week
  ORDER BY day_of_week;

--aggregation 4: month vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis04_month_vs AS
  SELECT EXTRACT(MONTH FROM started_at) AS month, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY month
  ORDER BY month;

--_____combination aggregations
--aggregation 5: member_casual + day_of_week vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis05_Combo1_membercasual_dayofweek_vs AS
  SELECT day_of_week, member_casual, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, member_casual
  ORDER BY day_of_week, member_casual;

--aggregation 6: member_casual + rideable type vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis06_Combo2_membercasual_rideabletype_vs AS
  SELECT member_casual, rideable_type, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY rideable_type, member_casual
  ORDER BY rideable_type, member_casual;

--aggregation 7: day_of_week + rideable_type vs ride length + count + mins spent riding
CREATE TABLE cyclistic_proj.Analysis07_Combo3_dayofweek_rideabletype_vs AS
  SELECT day_of_week, rideable_type, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, rideable_type
  ORDER BY day_of_week, rideable_type;

--aggregation 8: member_casual + month of year
CREATE TABLE cyclistic_proj.Analysis08_Combo4_membercasual_monthofyear_vs AS
  SELECT member_casual, EXTRACT(MONTH FROM started_at) AS month, ROUND(AVG(ride_length_in_mins),2) AS avg_ride_len_mins, ROUND(MAX(ride_length_in_mins),2) AS max_ride_len_mins, COUNT(*) AS counter, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY month, member_casual
  ORDER BY month, member_casual;

--____other aggregations suggested by Coursera document
--aggregation 9: mode day of the week
CREATE TABLE cyclistic_proj.Analysis09_modedayofweek AS
  SELECT day_of_week, COUNT (*) AS freq
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week
  ORDER BY freq DESC;

--aggregation 10: day of the week + member_casual vs numberofrides ordered by day only  (MODE)
CREATE TABLE cyclistic_proj.Analysis10_modedayofweek_membercasual_orderedfreq AS
  SELECT member_casual, day_of_week, COUNT (*) AS freq
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, member_casual
  ORDER BY freq DESC;

--aggregation 11: day of the week + member_casual vs numberofrides ordered by membercasual and day  (MODE)
CREATE TABLE cyclistic_proj.Analysis11_modedayofweek_membercasual_orderedmembercasual AS
  SELECT member_casual, day_of_week, COUNT (*) AS freq
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, member_casual
  ORDER BY member_casual, freq DESC;

--aggregation 12: day of the week + member_casual vs totalmins.   ordered by day only
CREATE TABLE cyclistic_proj.Analysis12_mostminsdayofweek_membercasual_orderedmostmins AS
  SELECT member_casual, day_of_week, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, member_casual
  ORDER BY total_ride_len_mins DESC;

--aggregation 13: mode day of the week + member_casual, ordered by membercasual and day
CREATE TABLE cyclistic_proj.Analysis13_mostminsdayofweek_membercasual_orderedmembercasual AS
  SELECT member_casual, day_of_week, ROUND(SUM(ride_length_in_mins), 2) AS total_ride_len_mins
  FROM secret-antonym-448215-m3.cyclistic_proj.Raw_Data_2024_AllMonths
  GROUP BY day_of_week, member_casual
  ORDER BY member_casual, total_ride_len_mins DESC;

# 1. How many vehicles are there in the C-Tran system?
SELECT DISTINCT vehicle_id FROM TRIP;

# 2. How many bread crumb reading events occurred on October 2, 2020?
SELECT
    COUNT(*)
FROM BreadCrumb
WHERE tstamp BETWEEN '2020-10-02 00:00:00-00'
    AND '2020-10-03 00:00:00-00';

# 3. How many bread crumb reading events occurred on October 3, 2020?
SELECT 
    COUNT(*)
FROM BreadCrumb
WHERE tstamp BETWEEN '2020-10-03 00:00:00-00'
    AND '2020-10-04 00:00:00-00';

# 4. On average, how many bread crumb readings are collected on each day of the week?
SELECT
    CASE
        WHEN EXTRACT (DOW FROM tstamp) = 0  THEN 'Sunday'
        WHEN EXTRACT (DOW FROM tstamp) = 1  THEN 'Monday'
        WHEN EXTRACT (DOW FROM tstamp) = 2  THEN 'Tuesday'
        WHEN EXTRACT (DOW FROM tstamp) = 3  THEN 'Wednesday'
        WHEN EXTRACT (DOW FROM tstamp) = 4  THEN 'Thursday'
        WHEN EXTRACT (DOW FROM tstamp) = 5  THEN 'Friday'
        WHEN EXTRACT (DOW FROM tstamp) = 6  THEN 'Saturday'
        ELSE NULL
    END as day_of_week,
    count(*)
    FROM BreadCrumb
    GROUP BY 1;

# 5. List the C-Tran trips that crossed the I-5 bridge on October 2, 2020. To find this, search for all trips that have bread crumb readings that occurred within a lat/lon bounding box such as [(45.620460, -122.677744), (45.615477, -122.673624)]. 

# 6. List all bread crumb readings for a specific portion of Highway 14 (bounding box: [(45.610794, -122.576979), (45.610794, -122.576979)]) during Mondays between 4pm and 6pm. Order the readings by tstamp. Then list readings for Sundays between 6am and 8am. How do these two time periods compare for this particular location?

# 7. What is the maximum velocity reached by any bus in the system?
SELECT MAX(speed) FROM BreadCrumb;

# 8. List all possible directions and give a count of the number of vehicles that faced precisely that direction during at least one trip. Sort the list by most frequent direction to least frequent.
SELECT
  COUNT(*),
  direction
FROM
  BreadCrumb
GROUP BY 2
ORDER BY 2 DESC;

# 9. (ignore question 9)

# 10. Which is the longest (in terms of time) trip of all trips in the data?
SELECT
  max(tstamp) - min(tstamp) AS DURATION,
  trip_id
FROM
  BreadCrumb
GROUP BY trip_id
ORDER BY 1 DESC
LIMIT 1;

# 11. (ignore question 11)

# 12. Devise three new, interesting questions about the C-Tran bus system that can be answered by your bread crumb data. Show your questions, their answers, the SQL you used to get the answers and the results of running the SQL queries on your data (the number of result rows, and first five rows returned).
# 12.a) What is the id of the route which has the shortest (in terms of time) trip of all trips in the data?
SELECT
  max(tstamp) - min(tstamp) AS DURATION,
  any_value(route_id)
FROM
  Trip
  LEFT JOIN BreadCrumb USING(trip_id)
GROUP BY trip_id
ORDER BY 1
LIMIT 1;

# 12.b) What are the top 10 vehicles which have the fastest average speed per trip of all trips?
SELECT
  avg(speed) as avg_speed,
  any_value(vehicle_id)
FROM Trip NATURAL JOIN BreadCrumb
GROUP BY trip_id
ORDER BY 1 DESC
LIMIT 10;

# 12.c) Did the trip with the most bread crumb events also take the most time? 
# If there is a result from the below, then yes. Otherwise no.
with most_crumbs as (
  select
    max(counting.crumbs) crumbs_per_trip counting.trip_id
  from
    (
      select
        count(*) crumbs,
        trip_id
      from test
      group by trip_id
    ) as counting
  group by trip_id
  order by 1 desc
  limit 1
),
longest_trip as (
  SELECT
    max(tstamp) - min(tstamp) AS DURATION,
    trip_id
  FROM BreadCrumb
  GROUP BY trip_id
  ORDER BY  1 DESC
  LIMIT 1
)
select
  *
from
  most_crumbs
  join longest_trip;


---

# Results

ctran=> SELECT count(DISTINCT vehicle_id) FROM TRIP;
 count 
-------
   105
(1 row)


ctran=> SELECT
ctran->     COUNT(*)
ctran-> FROM BreadCrumb
ctran-> WHERE tstamp BETWEEN '2020-10-02 00:00:00-00'
ctran->     AND '2020-10-03 00:00:00-00';
 count  
--------
 375413
(1 row)

ctran=> SELECT 
ctran->     COUNT(*)
ctran-> FROM BreadCrumb
ctran-> WHERE tstamp BETWEEN '2020-10-03 00:00:00-00'
ctran->     AND '2020-10-04 00:00:00-00';
 count  
--------
 177794
(1 row)


ctran=> SELECT MAX(speed) FROM BreadCrumb;
 max 
-----
 164
(1 row)




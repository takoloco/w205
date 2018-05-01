
# Query Project
- In the Query Project, you will get practice with SQL while learning about Google Cloud Platform (GCP) and BiqQuery. You'll answer business-driven questions using public datasets housed in GCP. To give you experience with different ways to use those datasets, you will use the web UI (BiqQuery) and the command-line tools, and work with them in jupyter notebooks.
- We will be using the Bay Area Bike Share Trips Data (https://cloud.google.com/bigquery/public-data/bay-bike-share). 

#### Query Project Problem Statement
- You're a data scientist at Ford GoBike (https://www.fordgobike.com/), the company running Bay Area Bikeshare. You are trying to increase ridership, and you want to offer deals through the mobile app to do so. What deals do you offer though? Currently, your company has three options: a flat price for a single one-way trip, a day pass that allows unlimited 30-minute rides for 24 hours and an annual membership. 

- Through this project, you will answer these questions: 
  * What are the 5 most popular trips that you would call "commuter trips"?
  * What are your recommendations for offers (justify based on your findings)?

_______________________________________________________________________________________________________________


## Assignment 04 - Querying data - Answer Your Project Questions

### Your Project Questions

- Answer at least 4 of the questions you identified last week.
- You can use either BigQuery or the bq command line tool.
- Paste your questions, queries and answers below.

#### Question 1: What are the peak hours for commuters (morning and evening)?

##### Answer

The peak hours are:
 * Morning: 7-10am
 * Evening: 4-7pm

##### SQL query
###### Morning

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%H",
    start_date) start_hour,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  start_hour
HAVING
  CAST(start_hour as INT64) <= 12
ORDER BY
  num DESC
LIMIT 3
```

| start_hour  | num | 
| ------------- | --- |
| 08 | 132464 |
| 09 | 96118 |
| 07 | 67531 |

###### Evening

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%H",
    start_date) start_hour,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  start_hour
HAVING
  CAST(start_hour as INT64) > 12
ORDER BY
  num DESC
LIMIT 3
```

| start_hour  | num | 
| ------------- | --- |
| 17 | 126302 |
| 16 | 88755 |
| 18 | 84569 |

#### Question 2: What is the most popular type of subscriber (subscriber vs customer) during the peak hours in Question 1?

##### Answer

There are considerably more subscribers than customers during the peak hours of 7-10am and 4-7pm.

##### SQL query

###### Morning

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%H",
    start_date) start_hour,
  subscriber_type,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  start_hour, subscriber_type
HAVING
  CAST(start_hour as INT64) BETWEEN 7 AND 10
ORDER BY
  num DESC
```

| start_hour  | subscriber_type | num | 
| ------------- | ------------- | --- |
| 08 | Subscriber | 127171 |
| 09 | Subscriber | 89546 |
| 07 | Subscriber | 64946 |
| 10 | Subscriber | 34532 |
| 10 | Customer | 8250 |
| 09 | Customer | 6572 |
| 08 | Customer | 5293 |
| 07 | Customer | 2585 |

###### Evening

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%H",
    start_date) start_hour,
  subscriber_type,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  start_hour,
  subscriber_type
HAVING
  CAST(start_hour AS INT64) BETWEEN 16 AND 19
ORDER BY
  num DESC
```

| start_hour  | subscriber_type | num | 
| ------------- | ------------- | --- |
| 17 | Subscriber | 114915 |
| 16 | Subscriber | 76051 |
| 18 | Subscriber | 75798 |
| 19 | Subscriber | 35515 |
| 16 | Customer | 12704 |
| 17 | Customer | 11387 |
| 18 | Customer | 8771 |
| 19 | Customer | 5556 |

#### Question 3: What is the average trip duration for commuters identified in Questioni 2?

##### Answer
 * Morning: 9.52 minutes
 * Evening: 9.84 minutes

##### SQL query

###### Morning

```sql
#standardSQL
SELECT
  FORMAT("%.2F", AVG(duration_sec)/60) trip_duration
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  CAST( FORMAT_TIMESTAMP("%H",
      start_date) AS INT64) BETWEEN 7 AND 10
  AND LOWER(subscriber_type) = 'subscriber'
```

| trip_duration | 
| ------------- |
| 9.52 |

###### Evening

```sql
#standardSQL
SELECT
  FORMAT("%.2F", AVG(duration_sec)/60) trip_duration
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  CAST( FORMAT_TIMESTAMP("%H",
      start_date) AS INT64) BETWEEN 16 AND 19
  AND LOWER(subscriber_type) = 'subscriber'
```

| trip_duration | 
| ------------- |
| 9.84 |

#### Question 4: What are the 5 most popular trips taken by the commuters identified in Question 2 by time of the day (morning vs evening)?

##### Answer

5 most popular trips taken by commtuters are as below:

 * Morning
   * Harry Bridges Plaza (Ferry Building) - 2nd at Townsend
   * San Francisco Caltrain 2 (330 Townsend) - Townsend at 7th
   * Steuart at Market - 2nd at Townsend
   * San Francisco Caltrain (Townsend at 4th) - Temporary Transbay Terminal (Howard at Beale)
   * San Francisco Caltrain (Townsend at 4th) - Embarcadero at Folsom
 * Evening
   * 2nd at Townsend - Harry Bridges Plaza (Ferry Building)
   * Embarcadero at Folsom - San Francisco Caltrain (Townsend at 4th)
   * Embarcadero at Sansome - Steuart at Market
   * 2nd at South Park - Market at Sansome
   * Steuart at Market - San Francisco Caltrain (Townsend at 4th)

##### SQL query

###### Morning

```sql
#standardSQL
SELECT
  start_station_name,
  end_station_name,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  CAST( FORMAT_TIMESTAMP("%H",
      start_date) AS INT64) BETWEEN 7 AND 10
  AND LOWER(subscriber_type) = 'subscriber'
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  num DESC
LIMIT
  5
```

| start_station_name | end_station_name | num | 
| ------------- | ------------- | --- |
| Harry Bridges Plaza (Ferry Building)	| 2nd at Townsend | 4605 |
| San Francisco Caltrain 2 (330 Townsend) | Townsend at 7th | 4103 |
| Steuart at Market | 2nd at Townsend | 4048 |
| San Francisco Caltrain (Townsend at 4th) | Temporary Transbay Terminal (Howard at Beale) | 3581 |
| San Francisco Caltrain (Townsend at 4th) | Embarcadero at Folsom | 3443 |

###### Evening

```sql
#standardSQL
SELECT
  start_station_name,
  end_station_name,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  CAST( FORMAT_TIMESTAMP("%H",
      start_date) AS INT64) BETWEEN 16 AND 19 
  AND LOWER(subscriber_type) = 'subscriber'
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  num DESC
LIMIT
  5
```

| start_station_name | end_station_name | num | 
| ------------- | ------------- | --- |
| 2nd at Townsend | Harry Bridges Plaza (Ferry Building) | 4268 |
| Embarcadero at Folsom | San Francisco Caltrain (Townsend at 4th) | 4088 |
| Embarcadero at Sansome | Steuart at Market | 4009 |
| 2nd at South Park | Market at Sansome | 3510 |
| Steuart at Market | San Francisco Caltrain (Townsend at 4th) | 3469 |

#### Question 5: What is the average trip duration for non-commuters?

##### Answer
The average trip duration for non-commuters is 61.98 minutes.

##### SQL query

```sql
#standardSQL
SELECT
  FORMAT("%.2F", AVG(duration_sec)/60) trip_duration
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  LOWER(subscriber_type) != 'subscriber'
  OR (CAST( FORMAT_TIMESTAMP("%H", start_date) AS INT64) NOT BETWEEN 7
    AND 9
    AND CAST( FORMAT_TIMESTAMP("%H", start_date) AS INT64) NOT BETWEEN 16
    AND 18
    AND subscriber_type = 'subscriber' )
```

#### Question 6: What are the 5 most popular trips taken by non-commuters?

##### Answer
5 popular trips taken by non-commuters are as below:
 * Harry Bridges Plaza (Ferry Building) - Embarcadero at Sansome
 * Embarcadero at Sansome - Embarcadero at Sansome
 * Harry Bridges Plaza (Ferry Building) - Harry Bridges Plaza (Ferry Building)
 * Embarcadero at Sansome - Harry Bridges Plaza (Ferry Building)
 * Embarcadero at Vallejo - Embarcadero at Sansome 

##### SQL query
```sql
#standardSQL
SELECT
  start_station_name,
  end_station_name,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE
  LOWER(subscriber_type) != 'subscriber'
  OR (CAST( FORMAT_TIMESTAMP("%H", start_date) AS INT64) NOT BETWEEN 7
    AND 9
    AND CAST( FORMAT_TIMESTAMP("%H", start_date) AS INT64) NOT BETWEEN 16
    AND 18
    AND subscriber_type = 'subscriber' )
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  num DESC
LIMIT
  5
```

| start_station_name | end_station_name | num | 
| ------------- | ------------- | --- |
| Harry Bridges Plaza (Ferry Building)	| Embarcadero at Sansome | 3667 |
| Embarcadero at Sansome | Embarcadero at Sansome | 2545 |
| Harry Bridges Plaza (Ferry Building)	| Harry Bridges Plaza (Ferry Building) | 2004 |
| Embarcadero at Sansome | Harry Bridges Plaza (Ferry Building) | 1638 |
| Embarcadero at Vallejo | Embarcadero at Sansome | 1345 |

# Query Project
- In the Query Project, you will get practice with SQL while learning about Google Cloud Platform (GCP) and BiqQuery. You'll answer business-driven questions using public datasets housed in GCP. To give you experience with different ways to use those datasets, you will use the web UI (BiqQuery) and the command-line tools, and work with them in jupyter notebooks.
- We will be using the Bay Area Bike Share Trips Data (https://cloud.google.com/bigquery/public-data/bay-bike-share). 

#### Problem Statement
- You're a data scientist at Ford GoBike (https://www.fordgobike.com/), the company running Bay Area Bikeshare. You are trying to increase ridership, and you want to offer deals through the mobile app to do so. What deals do you offer though? Currently, your company has three options: a flat price for a single one-way trip, a day pass that allows unlimited 30-minute rides for 24 hours and an annual membership. 

- Through this project, you will answer these questions: 
  * What are the 5 most popular trips that you would call "commuter trips"?
  * What are your recommendations for offers (justify based on your findings)?


## Assignment 02: Querying Data with BigQuery

### What is Google Cloud?
- Read: https://cloud.google.com/docs/overview/

### Get Going

- Go to https://cloud.google.com/bigquery/
- Click on "Try it Free"
- It asks for credit card, but you get $300 free and it does not autorenew after the $300 credit is used, so go ahead (OR CHANGE THIS IF SOME SORT OF OTHER ACCESS INFO)
- Now you will see the console screen. This is where you can manage everything for GCP
- Go to the menus on the left and scroll down to BigQuery
- Now go to https://cloud.google.com/bigquery/public-data/bay-bike-share 
- Scroll down to "Go to Bay Area Bike Share Trips Dataset" (This will open a BQ working page.)


### Some initial queries
Paste your SQL query and answer the question in a sentence.

####  What's the size of this dataset? (i.e., how many trips)

#####  Query
```sql
SELECT
  COUNT(*)
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips] 
```
##### Result
983648

#### What is the earliest start time and latest end time for a trip?

#####  Query
```sql
SELECT
  MIN(start_date) earliest,
  MAX(end_date) latest
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips]
```
##### Result
* earliest: 2013-08-29 09:08:00.000 UTC
* latest: 2016-08-31 23:48:00.000 UTC

#### How many bikes are there?

#####  Query
```sql
SELECT
  COUNT(DISTINCT bike_number) bike_num
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips] ;
```
##### Result
* bike_num: 700

### Questions of your own
- Make up 3 questions and answer them using the Bay Area Bike Share Trips Data.
- Use the SQL tutorial (https://www.w3schools.com/sql/default.asp) to help you with mechanics.

#### Question 1: Are there more trips taken by subscribers (annual or 30-day member) vs customers (24-hour or 3-day member)?

##### Answer
* Customer: 136809	 
* Subscriber: 846839

There are more trips taken by subscribers (annual or 30-day member).

#####  Query
```sql
SELECT
  subscriber_type,
  COUNT(*) trips
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips]
GROUP BY
  subscriber_type
ORDER BY
  subscriber_type
```

#### Question 2: Between subscribers and customers, which group is likely to go on longer rides?

##### Answer
* Customer: 3718.785160333019
* Subscriber: 582.7642397197106

Customers have a considerably higher average duration than subscribers.

#####  Query

```sql
SELECT
  subscriber_type,
  AVG(duration_sec) duration
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips]
GROUP BY
  subscriber_type
ORDER BY
  subscriber_type
```

#### Question 3: What are the most popular start station and end station?

##### Answer
* Start station
  * start_station_id: 70
  * start_station_name: San Francisco Caltrain (Townsend at 4th)	

* End station
  * end_station_id: 70
  * end_station_name: San Francisco Caltrain (Townsend at 4th)	

#####  Query
```sql
# Start station
SELECT
  start_station_id,
  start_station_name
FROM (
  SELECT
    start_station_id,
    start_station_name,
    COUNT(trip_id) trips
  FROM
    [bigquery-public-data:san_francisco.bikeshare_trips]
  GROUP BY
    start_station_id, start_station_name
  ORDER BY
    trips DESC
  LIMIT
    1 );
# End station
SELECT
  end_station_id,
  end_station_name
FROM (
  SELECT
    end_station_id,
    end_station_name,
    COUNT(trip_id) trips
  FROM
    [bigquery-public-data:san_francisco.bikeshare_trips]
  GROUP BY
    end_station_id, end_station_name
  ORDER BY
    trips DESC
  LIMIT
    1 );
```

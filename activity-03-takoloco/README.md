# template-activity-03


# Query Project
- In the Query Project, you will get practice with SQL while learning about Google Cloud Platform (GCP) and BiqQuery. You'll answer business-driven questions using public datasets housed in GCP. To give you experience with different ways to use those datasets, you will use the web UI (BiqQuery) and the command-line tools, and work with them in jupyter notebooks.
- We will be using the Bay Area Bike Share Trips Data (https://cloud.google.com/bigquery/public-data/bay-bike-share). 

#### Problem Statement
- You're a data scientist at Ford GoBike (https://www.fordgobike.com/), the company running Bay Area Bikeshare. You are trying to increase ridership, and you want to offer deals through the mobile app to do so. What deals do you offer though? Currently, your company has three options: a flat price for a single one-way trip, a day pass that allows unlimited 30-minute rides for 24 hours and an annual membership. 

- Through this project, you will answer these questions: 
  * What are the 5 most popular trips that you would call "commuter trips"?
  * What are your recommendations for offers (justify based on your findings)?


## Assignment 03 - Querying data from the BigQuery CLI - set up 

### What is Google Cloud SDK?
- Read: https://cloud.google.com/sdk/docs/overview

- If you want to go further, https://cloud.google.com/sdk/docs/concepts has lots of good stuff.

### Get Going

- Install Google Cloud SDK: https://cloud.google.com/sdk/docs/

- Try BQ from the command line:

  * General query structure

    bq query --use_legacy_sql=false '
        SELECT count(*)
        FROM
           `bigquery-public-data.san_francisco.bikeshare_trips`'

### Result
```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
SELECT count(*) FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
Waiting on bqjob_r5a99f4054004d291_0000016174c63cbd_1 ... (0s) Current status: DONE   
+--------+
|  f0_   |
+--------+
| 983648 |
+--------+
```


### Queries

1. Rerun last week's queries using bq command line tool:
  * Paste your bq queries:

- What's the size of this dataset? (i.e., how many trips)

#### Query

```sql
SELECT
  COUNT(*)
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips]
```

#### bq Result

```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
SELECT
  COUNT(*)
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
> '
Waiting on bqjob_r6b701094a8886198_0000016174d21c7f_1 ... (0s) Current status: DONE   
+--------+
|  f0_   |
+--------+
| 983648 |
+--------+
```

- What is the earliest start time and latest end time for a trip?

#### Query

```sql
SELECT
  MIN(start_date) earliest,
  MAX(end_date) latest
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
```

#### bq Result

```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
SELECT
>   MIN(start_date) earliest,
>   MAX(end_date) latest
> FROM
>   `bigquery-public-data.san_francisco.bikeshare_trips`
> '
Waiting on bqjob_r731cd7cbf1129da4_0000016174d5bc91_1 ... (0s) Current status: DONE   
+---------------------+---------------------+
|      earliest       |       latest        |
+---------------------+---------------------+
| 2013-08-29 09:08:00 | 2016-08-31 23:48:00 |
+---------------------+---------------------+
```

- How many bikes are there?

#### Query

```sql
SELECT
  COUNT(DISTINCT bike_number) bike_num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
```

#### bq Query

```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
SELECT
>   COUNT(DISTINCT bike_number) bike_num
> FROM
>   `bigquery-public-data.san_francisco.bikeshare_trips`
> '
Waiting on bqjob_r735530219365ee2d_0000016174da5111_1 ... (0s) Current status: DONE   
+----------+
| bike_num |
+----------+
|      700 |
+----------+
```


2. New Query
  * Paste your SQL query and answer the question in a sentence.

- How many trips are in the morning vs in the afternoon?

#### Query

##### Start Date

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%p",
    start_date) ampm,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  ampm
```

##### End Date

```sql
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%p",
    end_date) ampm,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  ampm
```

#### bq Query

```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
> #standardSQL
> SELECT
>   FORMAT_TIMESTAMP("%p",
>     start_date) ampm,
>   COUNT(*) num
> FROM
>   `bigquery-public-data.san_francisco.bikeshare_trips`
> GROUP BY
>   ampm
> '
Waiting on bqjob_r268cfdeed6b2c03a_000001617527562a_1 ... (1s) Current status: DONE   
+------+--------+
| ampm |  num   |
+------+--------+
| PM   | 571309 |
| AM   | 412339 |
+------+--------+
```

There were 571,309 trips in the morning (AM) and 412,339 trips in the afternoon (PM) based on their start times.

```bash
root@3742de64eabf:/# bq query --use_legacy_sql=false '
#standardSQL
SELECT
  FORMAT_TIMESTAMP("%p",
    end_date) ampm,
  COUNT(*) num
FROM
  `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY
  ampm
'
Waiting on bqjob_r4cd22438f947c728_00000161752cecaa_1 ... (3s) Current status: DONE   
+------+--------+
| ampm |  num   |
+------+--------+
| PM   | 583504 |
| AM   | 400144 |
+------+--------+
```

There were 583,504 trips in the morning (AM) and 400,144 trips in the afternoon (PM) based on their end times.

### Project Questions
- Identify the main questions you'll need to answer to make recommendations (list below, add as many questions as you need). You'll answer these questions in a later assignment.

  * Question 1: What are the peak hours for commuters (morning and evening)?
  * Question 2: What is the most popular type of subscriber (subscriber vs customer) during the peak hours in Question 1?
  * Question 3: What is the average trip duration for commuters identified in Questioni 2?
  * Question 4: What are the 5 most popular trips taken by the commuters identified in Question 2 by time of the day (morning vs evening)? 
  * Question 5: What is the average trip duration for non-commuters?
  * Question 6: What are the 5 most popular trips taken by non-commuters?


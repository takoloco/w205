# My annotations, Assignment 8 #

## Move into the spark-with-kafka-and-hdfs directory ##

    1  cd w205/spark-with-kafka-and-hdfs/

## Copy docker-compose.yml from the synch session working directory to the current working directory ##
    2  cp -ip ../course-content/08-Querying-Data/docker-compose.yml .

## Modify docker-compose.yml to use the absolute path instead of ~ in referencing the home directory ##
    3  vim docker-compose.yml
    4  cd ..

## Download a JSON file from the specified location with the curl command ##

    5  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4
    6  cd spark-with-kafka-and-hdfs/
    7  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t exams"
    8  vim docker-compose.yml

## Start up the cluster ##
    9  docker-compose up -d
   10  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t exams"
   11  vim docker-compose.yml
   12  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t exams"

## Create a new topic "exams" ##
   13  docker-compose exec kafka kafka-topics --create --topic exams --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181

## Publish data ##
   14  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t exams"

## Start up pyspark to start consuming published exam data ##
   15  docker-compose exec spark pyspark

### Consume exams data into a Kafka data frame

```python
raw_exams = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe","exams").option("startingOffsets", "earliest").option("endingOffsets", "latest").load() 
```

### Cache the data ###

```python
raw_exams.cache()
```

### Confirm the schema ###

```python
raw_exams.printSchema()
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

### Convert the raw data into strings ###

```python
exams = raw_exams.select(raw_players.value.cast('string'))
```

### Confirm the data ###

```python
exams.show()
+--------------------+
|               value|
+--------------------+
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
+--------------------+
```

### Dump the data into a file ###

```python
exams.write.parquet("/tmp/exams")
```

### Set standard output to write data using utf-8 instead of unicode ###

```python
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
```

### Create a new data frame to hold the data in json format ###

```python
extracted_exams = exams.rdd.map(lambda x: json.loads(x.value)).toDF()
```

### Confirm the scheme ###

```python
>>> extracted_exams.printSchema()
root
 |-- base_exam_id: string (nullable = true)
 |-- certification: string (nullable = true)
 |-- exam_name: string (nullable = true)
 |-- keen_created_at: string (nullable = true)
 |-- keen_id: string (nullable = true)
 |-- keen_timestamp: string (nullable = true)
 |-- max_attempts: string (nullable = true)
 |-- sequences: map (nullable = true)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = true)
 |    |    |-- element: map (containsNull = true)
 |    |    |    |-- key: string
 |    |    |    |-- value: boolean (valueContainsNull = true)
 |-- started_at: string (nullable = true)
 |-- user_exam_id: string (nullable = true)
```

### Confirm the data ###

```python
extracted_exmas.show()
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
|        base_exam_id|certification|           exam_name|   keen_created_at|             keen_id|    keen_timestamp|max_attempts|           sequences|          started_at|        user_exam_id|
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717442.735266|5a6745820eb8ab000...| 1516717442.735266|         1.0|Map(questions -> ...|2018-01-23T14:23:...|6d4089e4-bde5-4a2...|
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717377.639827|5a674541ab6b0a000...| 1516717377.639827|         1.0|Map(questions -> ...|2018-01-23T14:21:...|2fec1534-b41f-441...|
|4beeac16-bb83-4d5...|        false|The Principles of...| 1516738973.653394|5a67999d3ed3e3000...| 1516738973.653394|         1.0|Map(questions -> ...|2018-01-23T20:22:...|8edbc8a8-4d26-429...|
|4beeac16-bb83-4d5...|        false|The Principles of...|1516738921.1137421|5a6799694fc7c7000...|1516738921.1137421|         1.0|Map(questions -> ...|2018-01-23T20:21:...|c0ee680e-8892-4e6...|
|6442707e-7488-11e...|        false|Introduction to B...| 1516737000.212122|5a6791e824fccd000...| 1516737000.212122|         1.0|Map(questions -> ...|2018-01-23T19:48:...|e4525b79-7904-405...|
|8b4488de-43a5-4ff...|        false|        Learning Git| 1516740790.309757|5a67a0b6852c2a000...| 1516740790.309757|         1.0|Map(questions -> ...|2018-01-23T20:51:...|3186dafa-7acf-47e...|
|e1f07fac-5566-4fd...|        false|Git Fundamentals ...|1516746279.3801291|5a67b627cc80e6000...|1516746279.3801291|         1.0|Map(questions -> ...|2018-01-23T22:24:...|48d88326-36a3-4cb...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743820.305464|5a67ac8cb0a5f4000...| 1516743820.305464|         1.0|Map(questions -> ...|2018-01-23T21:43:...|bb152d6b-cada-41e...|
|1a233da8-e6e5-48a...|        false|Intermediate Pyth...|  1516743098.56811|5a67a9ba060087000...|  1516743098.56811|         1.0|Map(questions -> ...|2018-01-23T21:31:...|70073d6f-ced5-4d0...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743764.813107|5a67ac54411aed000...| 1516743764.813107|         1.0|Map(questions -> ...|2018-01-23T21:42:...|9eb6d4d6-fd1f-4f3...|
|4cdf9b5f-fdb7-4a4...|        false|A Practical Intro...|1516744091.3127241|5a67ad9b2ff312000...|1516744091.3127241|         1.0|Map(questions -> ...|2018-01-23T21:45:...|093f1337-7090-457...|
|e1f07fac-5566-4fd...|        false|Git Fundamentals ...|1516746256.5878439|5a67b610baff90000...|1516746256.5878439|         1.0|Map(questions -> ...|2018-01-23T22:24:...|0f576abb-958a-4c0...|
|87b4b3f9-3a86-435...|        false|Introduction to M...|  1516743832.99235|5a67ac9837b82b000...|  1516743832.99235|         1.0|Map(questions -> ...|2018-01-23T21:40:...|0c18f48c-0018-450...|
|a7a65ec6-77dc-480...|        false|   Python Epiphanies|1516743332.7596769|5a67aaa4f21cc2000...|1516743332.7596769|         1.0|Map(questions -> ...|2018-01-23T21:34:...|b38ac9d8-eef9-495...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743750.097306|5a67ac46f7bce8000...| 1516743750.097306|         1.0|Map(questions -> ...|2018-01-23T21:41:...|bbc9865f-88ef-42e...|
|e5602ceb-6f0d-11e...|        false|Python Data Struc...|1516744410.4791961|5a67aedaf34e85000...|1516744410.4791961|         1.0|Map(questions -> ...|2018-01-23T21:51:...|8a0266df-02d7-44e...|
|e5602ceb-6f0d-11e...|        false|Python Data Struc...|1516744446.3999851|5a67aefef5e149000...|1516744446.3999851|         1.0|Map(questions -> ...|2018-01-23T21:53:...|95d4edb1-533f-445...|
|f432e2e3-7e3a-4a7...|        false|Working with Algo...| 1516744255.840405|5a67ae3f0c5f48000...| 1516744255.840405|         1.0|Map(questions -> ...|2018-01-23T21:50:...|f9bc1eff-7e54-42a...|
|76a682de-6f0c-11e...|        false|Learning iPython ...| 1516744023.652257|5a67ad579d5057000...| 1516744023.652257|         1.0|Map(questions -> ...|2018-01-23T21:46:...|dc4b35a7-399a-4bd...|
|a7a65ec6-77dc-480...|        false|   Python Epiphanies|1516743398.6451161|5a67aae6753fd6000...|1516743398.6451161|         1.0|Map(questions -> ...|2018-01-23T21:35:...|d0f8249a-597e-4e1...|
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
only showing top 20 rows
```

### Dump the data into a file ###

```python
extracted_exams.write.parquet("/tmp/extracted_exams")
```

### Crete a spark temporary table called exams based on the data frame ###

```python
extracted_exams.registerTempTable('exams')
```

### Run a SELECT statement to fetch 10 base_exam_ids ### 

```python
spark.sql("select exams.base_exam_id from exams limit 10").show()
+--------------------+
|        base_exam_id|
+--------------------+
|37f0a30a-7464-11e...|
|37f0a30a-7464-11e...|
|4beeac16-bb83-4d5...|
|4beeac16-bb83-4d5...|
|6442707e-7488-11e...|
|8b4488de-43a5-4ff...|
|e1f07fac-5566-4fd...|
|7e2e0b53-a7ba-458...|
|1a233da8-e6e5-48a...|
|7e2e0b53-a7ba-458...|
+--------------------+
```

### Run a SELECT statement to fetch 10 questions ###

```python
spark.sql("select exams.sequences.questions from exams limit 10").show()
+--------------------+
|           questions|
+--------------------+
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
|[Map(user_incompl...|
+--------------------+
```

### Run a SELECT statement to retrieve some exam data ###

```python
some_exams = spark.sql("select base_exam_id, exam_name, user_exam_id from exams limit 10")
```

### Confirm the variable some_exams###

```python
some_exams
DataFrame[base_exam_id: string, exam_name: string, user_exam_id: string]
```

### Dump the data into a file ###

```python
some_exams.write.parquet("/tmp/some_exams")
```

### Exit pyspark ###

```python
exit()
```

## Bring down the cluster ##
   16  docker-compose down

## Confirm all the docker-compose processes have been shut down properly ##y
   17  docker-compose ps
   18  docker ps -a
   19  history > takoloco-history.txt

# Project 3 Setup

- You're a data scientist at a game development company.  
- Your latest mobile game has two events you're interested in tracking: 
- `buy a sword` & `join guild`...
- Each has metadata

## Project 3 Task
- Your task: instrument your API server to catch and analyze event types.
- This task will be spread out over the last four assignments (9-12).

## Project 3 Task Options 

- All: Game shopping cart data used for homework 
- Advanced option 1: Generate and filter more types of items.
- Advanced option 2: Enhance the API to accept parameters for purchases (sword/item type) and filter
- Advanced option 3: Shopping cart data & track state (e.g., user's inventory) and filter


---

# Assignment 12

## Do the following:
- Spin up the cluster with the necessary containers.
- Run your web app.
- Generate data.
- Write events & check to make sure you were successful.
- Run queries with Presto - at least Select * from <your-table-name>

### Turn in your `/assignment-12-<user-name>/README.md` file. It should include:
#### 1) A summary type explanation of the example. ####
 * Start up the docker cluster
 * Create a python web server script using the flask library
 * Start up the web server
 * Create a topic on Kafka
 * Start up kafkacat in the continous mode
 * Access the web server via the Apache Bench command
 * Create a python script for extracting events from kafka and filtering them for purchase events only and writes to HDFS using Spark
 * Execute filtered_writes.py to extract events from kafka via spark-submit
 * Verify that the files have been written to HDFS
 * Create a pyspark script that will impose schema-on-read on the parquet files for spark SQL
 * Start up Hive
 * Run python spark code using pyspark
 * Confirm that the data has been stored in purchases table using hive
 * Verify that the files have been written to HDFS
 * Start up Presto
 * Play around with Presto
 * Terminate running processes
 * Take down the cluster
 * Confirm all containers have been properly shut down
#### 2) Your `docker-compose.yml` ####
#### 3) Source code for the application(s) used. ####
#### 4) Each important step in the process. For each step, include: ####
  * The command(s) 
  * The output (if there is any).  Be sure to include examples of generated events when available.
  * An explanation for what it achieves 
    * The explanation should be fairly detailed, e.g., instead of "publish to kafka" say what you're publishing, where it's coming from, going to etc.

##### Start up the docker cluster #####

```bash
$ docker-compose up -d
```
```
Creating network "activity12takoloco_default" with the default driver
Creating activity12takoloco_presto_1
Creating activity12takoloco_mids_1
Creating activity12takoloco_zookeeper_1
Creating activity12takoloco_cloudera_1
Creating activity12takoloco_kafka_1
Creating activity12takoloco_spark_1
```

Start up the Docker cluster defined in the docker-compose.yaml which include the following services:

* zookeeper
* kafka
* cloudera
* spark
* presto
* mids

##### Create a python web server script using the flask library #####

```bash
$ vim game_api.py
```

```python
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"
```

Create a file called game_api.py and save the file to the file system after editing

##### Start up the web server #####

```bash
$ docker-compose exec mids env FLASK_APP=/w205/activity-12-takoloco/game_api.py flask run --host 0.0.0.0
```

```
 * Serving Flask app "game_api"
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Start up a Flask web server process with the Python file /w205/activity-12-takoloco/game_api.py

##### Create a topic on Kafka #####

```bash
$ docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```
```
Created topic "events".
```

Create a Kafka topic called "events"

##### Start up kafkacat in the continous mode #####

```bash
$ docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
```

kafkacat is started in the continous mode to consume messages as they come in

##### Access the web server via the Apache Bench command #####

```bash
$ docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/
```

###### Client ######

```
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient).....done


Server Software:        Werkzeug/0.14.1
Server Hostname:        localhost
Server Port:            5000

Document Path:          /
Document Length:        30 bytes

Concurrency Level:      1
Time taken for tests:   0.103 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1850 bytes
HTML transferred:       300 bytes
Requests per second:    97.19 [#/sec] (mean)
Time per request:       10.289 [ms] (mean)
Time per request:       10.289 [ms] (mean, across all concurrent requests)
Transfer rate:          17.56 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.1      0       0
Processing:     2    9   5.2      9      19
Waiting:        1    8   5.5      8      19
Total:          2   10   5.2      9      19

Percentage of the requests served within a certain time (ms)
  50%      9
  66%     10
  75%     13
  80%     16
  90%     19
  95%     19
  98%     19
  99%     19
 100%     19 (longest request)
```

###### Server ######
```
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 18:47:37] "GET / HTTP/1.0" 200 -
```

###### Kafkacat ######

```
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
```

Make 10 HTTP GET requests against the localhost on the port 5000 at the location "/" with the customer header "Host": "user1.comcast.com". The web server publishes the "default" event to Kafka topic "events" which Kafkacat consumes and processes.


```bash
$ docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword
```

###### Client ######

```
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient).....done


Server Software:        Werkzeug/0.14.1
Server Hostname:        localhost
Server Port:            5000

Document Path:          /purchase_a_sword
Document Length:        17 bytes

Concurrency Level:      1
Time taken for tests:   0.138 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1720 bytes
HTML transferred:       170 bytes
Requests per second:    72.31 [#/sec] (mean)
Time per request:       13.830 [ms] (mean)
Time per request:       13.830 [ms] (mean, across all concurrent requests)
Transfer rate:          12.15 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     2   14   9.6     12      39
Waiting:        2   12   9.0     12      34
Total:          2   14   9.6     12      39

Percentage of the requests served within a certain time (ms)
  50%     12
  66%     14
  75%     14
  80%     16
  90%     39
  95%     39
  98%     39
  99%     39
 100%     39 (longest request)
```

###### Server ######

```
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:20:16] "GET /purchase_a_sword HTTP/1.0" 200 -
```

###### Kafkacat ######

```
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user1.comcast.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
```

Make 10 HTTP GET requests against the localhost on the port 5000 at the location "/purchase_a_sword" with the customer header "Host": "user1.comcast.com". The web server publishes the "purchase_sword" event to Kafka topic "events" which Kafkacat consumes and processes.

```bash
$ docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/
```

###### Client ######

```
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient).....done


Server Software:        Werkzeug/0.14.1
Server Hostname:        localhost
Server Port:            5000

Document Path:          /
Document Length:        30 bytes

Concurrency Level:      1
Time taken for tests:   0.125 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1850 bytes
HTML transferred:       300 bytes
Requests per second:    79.81 [#/sec] (mean)
Time per request:       12.530 [ms] (mean)
Time per request:       12.530 [ms] (mean, across all concurrent requests)
Transfer rate:          14.42 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     2   12  11.8     11      37
Waiting:        1   11  11.2     10      36
Total:          2   12  11.8     11      37

Percentage of the requests served within a certain time (ms)
  50%     11
  66%     18
  75%     21
  80%     23
  90%     37
  95%     37
  98%     37
  99%     37
 100%     37 (longest request)
```

###### Server ######

```
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:28:39] "GET / HTTP/1.0" 200 -
```

###### Kafkacat ######

```
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "default", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
```

Make 10 HTTP GET requests against the localhost on the port 5000 at the location "/" with the customer header "Host": "user2.att.com". The web server publishes the "default" event to Kafka topic "events" 10 times which Kafkacat consumes and processes.

```bash
$ docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword
```

###### Client ######

```
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient).....done


Server Software:        Werkzeug/0.14.1
Server Hostname:        localhost
Server Port:            5000

Document Path:          /purchase_a_sword
Document Length:        17 bytes

Concurrency Level:      1
Time taken for tests:   0.101 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1720 bytes
HTML transferred:       170 bytes
Requests per second:    99.42 [#/sec] (mean)
Time per request:       10.058 [ms] (mean)
Time per request:       10.058 [ms] (mean, across all concurrent requests)
Transfer rate:          16.70 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     2   10  10.8      3      33
Waiting:        2    8  11.1      2      33
Total:          2   10  10.8      3      33

Percentage of the requests served within a certain time (ms)
  50%      3
  66%     15
  75%     17
  80%     20
  90%     33
  95%     33
  98%     33
  99%     33
 100%     33 (longest request)
```

###### Server ######

```
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [13/Apr/2018 19:30:37] "GET /purchase_a_sword HTTP/1.0" 200 -
```

###### Kafkacat ######

```
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
{"Host": "user2.att.com", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3"}
```

Make 10 HTTP GET requests against the localhost on the port 5000 at the location "/purchase_a_sword" with the customer header "Host": "user2.att.com". The web server publishes the "purchase_sword" event to the Kafka topic "events" 10 times which Kafkacat consumes and processes.

##### Create a python script for extracting events from kafka and filtering them for purchase events only and writes to HDFS using Spark #####

```bash
$ vim ~/w205/full-stack/filtered_writes.py
```

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_events = purchase_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_events.printSchema()
    extracted_purchase_events.show()

    extracted_purchase_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/purchases')


if __name__ == "__main__":
    main()
```

Create a file called filtered_writes.py and save the fiee to the file system after editing

##### Execute filtered_writes.py to extract events from kafka via spark-submit #####

```bash
$ docker-compose exec spark spark-submit /w205/activity-12-takoloco/filtered_writes.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/04/13 23:00:01 INFO SparkContext: Running Spark version 2.2.0
18/04/13 23:00:03 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/13 23:00:03 INFO SparkContext: Submitted application: ExtractEventsJob
18/04/13 23:00:03 INFO SecurityManager: Changing view acls to: root
18/04/13 23:00:03 INFO SecurityManager: Changing modify acls to: root
18/04/13 23:00:03 INFO SecurityManager: Changing view acls groups to: 
18/04/13 23:00:03 INFO SecurityManager: Changing modify acls groups to: 
18/04/13 23:00:03 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/04/13 23:00:05 INFO Utils: Successfully started service 'sparkDriver' on port 45112.
18/04/13 23:00:05 INFO SparkEnv: Registering MapOutputTracker
18/04/13 23:00:05 INFO SparkEnv: Registering BlockManagerMaster
18/04/13 23:00:05 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/04/13 23:00:05 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/04/13 23:00:05 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-52556ac1-4336-4c7c-829d-b183a50f3e55
18/04/13 23:00:06 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/04/13 23:00:06 INFO SparkEnv: Registering OutputCommitCoordinator
18/04/13 23:00:07 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/04/13 23:00:07 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.7:4040
18/04/13 23:00:08 INFO SparkContext: Added file file:/w205/activity-12-takoloco/filtered_writes.py at file:/w205/activity-12-takoloco/filtered_writes.py with timestamp 1523660408177
18/04/13 23:00:08 INFO Utils: Copying /w205/activity-12-takoloco/filtered_writes.py to /tmp/spark-33fff78d-9971-4090-b1e9-4443be1e3ef0/userFiles-98687211-8fbe-4d7c-a4ea-8b3ac9cb005a/filtered_writes.py
18/04/13 23:00:08 INFO Executor: Starting executor ID driver on host localhost
18/04/13 23:00:08 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 43245.
18/04/13 23:00:08 INFO NettyBlockTransferService: Server created on 172.18.0.7:43245
18/04/13 23:00:08 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/04/13 23:00:08 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.7, 43245, None)
18/04/13 23:00:08 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.7:43245 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.7, 43245, None)
18/04/13 23:00:09 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.7, 43245, None)
18/04/13 23:00:09 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.7, 43245, None)
18/04/13 23:00:10 INFO SharedState: loading hive config file: file:/spark-2.2.0-bin-hadoop2.6/conf/hive-site.xml
18/04/13 23:00:10 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse').
18/04/13 23:00:10 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse'.
18/04/13 23:00:13 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/04/13 23:00:19 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:00:19 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:00:20 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 1
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = earliest

18/04/13 23:00:21 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-1
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 1
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = earliest

18/04/13 23:00:21 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:00:21 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:00:25 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0.
18/04/13 23:00:25 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0
18/04/13 23:00:25 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0
18/04/13 23:00:28 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0 with generation 1
18/04/13 23:00:28 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-driver-0
18/04/13 23:00:28 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/04/13 23:00:31 INFO CodeGenerator: Code generated in 696.728014 ms
18/04/13 23:00:31 INFO CodeGenerator: Code generated in 27.439347 ms
18/04/13 23:00:36 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/04/13 23:00:36 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/04/13 23:00:36 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/04/13 23:00:36 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:00:36 INFO DAGScheduler: Missing parents: List()
18/04/13 23:00:36 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/04/13 23:00:36 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 22.5 KB, free 366.3 MB)
18/04/13 23:00:36 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.8 KB, free 366.3 MB)
18/04/13 23:00:36 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.7:43245 (size: 10.8 KB, free: 366.3 MB)
18/04/13 23:00:36 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/04/13 23:00:37 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/04/13 23:00:37 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/04/13 23:00:37 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:00:37 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/04/13 23:00:37 INFO Executor: Fetching file:/w205/activity-12-takoloco/filtered_writes.py with timestamp 1523660408177
18/04/13 23:00:37 INFO Utils: /w205/activity-12-takoloco/filtered_writes.py has been previously copied to /tmp/spark-33fff78d-9971-4090-b1e9-4443be1e3ef0/userFiles-98687211-8fbe-4d7c-a4ea-8b3ac9cb005a/filtered_writes.py
18/04/13 23:00:37 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:37 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-2
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:37 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:00:37 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:00:37 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor.
18/04/13 23:00:38 INFO CodeGenerator: Code generated in 101.035813 ms
18/04/13 23:00:38 INFO CodeGenerator: Code generated in 127.236301 ms
18/04/13 23:00:38 INFO CodeGenerator: Code generated in 82.275855 ms
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:39 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:40 INFO CodeGenerator: Code generated in 81.73228 ms
18/04/13 23:00:40 INFO PythonRunner: Times: total = 1600, boot = 1399, init = 198, finish = 3
18/04/13 23:00:40 INFO PythonRunner: Times: total = 61, boot = 11, init = 49, finish = 1
18/04/13 23:00:40 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2136 bytes result sent to driver
18/04/13 23:00:40 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 3380 ms on localhost (executor driver) (1/1)
18/04/13 23:00:40 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/04/13 23:00:40 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 3.524 s
18/04/13 23:00:40 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 4.477602 s
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- timestamp: string (nullable = true)

18/04/13 23:00:41 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/04/13 23:00:41 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/13 23:00:41 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/04/13 23:00:41 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:00:41 INFO DAGScheduler: Missing parents: List()
18/04/13 23:00:41 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/13 23:00:41 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 27.6 KB, free 366.2 MB)
18/04/13 23:00:41 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 13.5 KB, free 366.2 MB)
18/04/13 23:00:41 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.7:43245 (size: 13.5 KB, free: 366.3 MB)
18/04/13 23:00:41 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/04/13 23:00:41 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/13 23:00:41 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/04/13 23:00:41 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:00:41 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/04/13 23:00:41 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:41 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-3
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:41 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:00:41 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:00:41 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor.
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:41 INFO PythonRunner: Times: total = 177, boot = -1374, init = 1550, finish = 1
18/04/13 23:00:41 INFO CodeGenerator: Code generated in 95.531544 ms
18/04/13 23:00:41 INFO PythonRunner: Times: total = 96, boot = 20, init = 53, finish = 23
18/04/13 23:00:41 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2247 bytes result sent to driver
18/04/13 23:00:41 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 620 ms on localhost (executor driver) (1/1)
18/04/13 23:00:41 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/04/13 23:00:41 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) finished in 0.621 s
18/04/13 23:00:41 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.733100 s
18/04/13 23:00:42 INFO CodeGenerator: Code generated in 155.428176 ms
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
+------+-----------------+---------------+--------------+--------------------+
only showing top 20 rows

18/04/13 23:00:45 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.18.0.7:43245 in memory (size: 13.5 KB, free: 366.3 MB)
18/04/13 23:00:45 INFO ContextCleaner: Cleaned accumulator 54
18/04/13 23:00:45 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:00:45 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:00:45 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:00:45 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
18/04/13 23:00:45 INFO DAGScheduler: Got job 2 (parquet at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/13 23:00:45 INFO DAGScheduler: Final stage: ResultStage 2 (parquet at NativeMethodAccessorImpl.java:0)
18/04/13 23:00:45 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:00:45 INFO DAGScheduler: Missing parents: List()
18/04/13 23:00:45 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[19] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/13 23:00:45 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 92.3 KB, free 366.2 MB)
18/04/13 23:00:45 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 36.5 KB, free 366.1 MB)
18/04/13 23:00:45 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.7:43245 (size: 36.5 KB, free: 366.3 MB)
18/04/13 23:00:45 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1006
18/04/13 23:00:45 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[19] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/13 23:00:45 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
18/04/13 23:00:45 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:00:46 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
18/04/13 23:00:46 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:46 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-4
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:00:46 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:00:46 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:00:46 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-adcfd1c8-4b70-406c-8327-30e959788293-executor.
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:00:46 INFO PythonRunner: Times: total = 115, boot = -4689, init = 4798, finish = 6
18/04/13 23:00:46 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:00:46 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:00:46 INFO CodecConfig: Compression: SNAPPY
18/04/13 23:00:46 INFO CodecConfig: Compression: SNAPPY
18/04/13 23:00:46 INFO ParquetOutputFormat: Parquet block size to 134217728
18/04/13 23:00:46 INFO ParquetOutputFormat: Parquet page size to 1048576
18/04/13 23:00:46 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
18/04/13 23:00:46 INFO ParquetOutputFormat: Dictionary is on
18/04/13 23:00:46 INFO ParquetOutputFormat: Validation is off
18/04/13 23:00:46 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
18/04/13 23:00:46 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
18/04/13 23:00:46 INFO ParquetOutputFormat: Page size checking is: estimated
18/04/13 23:00:46 INFO ParquetOutputFormat: Min row count for page size check is: 100
18/04/13 23:00:46 INFO ParquetOutputFormat: Max row count for page size check is: 10000
18/04/13 23:00:46 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "Accept",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Host",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "User-Agent",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "event_type",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "timestamp",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary Accept (UTF8);
  optional binary Host (UTF8);
  optional binary User-Agent (UTF8);
  optional binary event_type (UTF8);
  optional binary timestamp (UTF8);
}

       
18/04/13 23:00:47 INFO CodecPool: Got brand-new compressor [.snappy]
18/04/13 23:00:47 INFO PythonRunner: Times: total = 77, boot = -4690, init = 4755, finish = 12
18/04/13 23:00:47 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 1489
18/04/13 23:00:50 INFO FileOutputCommitter: Saved output of task 'attempt_20180413230046_0002_m_000000_0' to hdfs://cloudera/tmp/purchases/_temporary/0/task_20180413230046_0002_m_000000
18/04/13 23:00:50 INFO SparkHadoopMapRedUtil: attempt_20180413230046_0002_m_000000_0: Committed
18/04/13 23:00:50 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2325 bytes result sent to driver
18/04/13 23:00:50 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 4453 ms on localhost (executor driver) (1/1)
18/04/13 23:00:50 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/04/13 23:00:50 INFO DAGScheduler: ResultStage 2 (parquet at NativeMethodAccessorImpl.java:0) finished in 4.485 s
18/04/13 23:00:50 INFO DAGScheduler: Job 2 finished: parquet at NativeMethodAccessorImpl.java:0, took 4.699632 s
18/04/13 23:00:50 INFO FileFormatWriter: Job null committed.
18/04/13 23:00:51 INFO SparkContext: Invoking stop() from shutdown hook
18/04/13 23:00:51 INFO SparkUI: Stopped Spark web UI at http://172.18.0.7:4040
18/04/13 23:00:51 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/04/13 23:00:51 INFO MemoryStore: MemoryStore cleared
18/04/13 23:00:51 INFO BlockManager: BlockManager stopped
18/04/13 23:00:51 INFO BlockManagerMaster: BlockManagerMaster stopped
18/04/13 23:00:51 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/04/13 23:00:51 INFO SparkContext: Successfully stopped SparkContext
18/04/13 23:00:51 INFO ShutdownHookManager: Shutdown hook called
18/04/13 23:00:51 INFO ShutdownHookManager: Deleting directory /tmp/spark-33fff78d-9971-4090-b1e9-4443be1e3ef0/pyspark-4166c348-b857-4055-982d-b19843a24186
18/04/13 23:00:51 INFO ShutdownHookManager: Deleting directory /tmp/spark-33fff78d-9971-4090-b1e9-4443be1e3ef0
```

The script extracts all purchase events from Kafka and prints them out as well as its schema and writes them out to HDFS.


###### Schema ######

```
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- timestamp: string (nullable = true)
```

###### Events ######

```
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
+------+-----------------+---------------+--------------+--------------------+
only showing top 20 row
```

##### Verify that the files have been written to HDFS #####

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/
```

```
Found 4 items
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2018-04-13 18:15 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2018-04-13 23:00 /tmp/purchases
```

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases/
```

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-04-13 23:00 /tmp/purchases/_SUCCESS
-rw-r--r--   1 root supergroup       1724 2018-04-13 23:00 /tmp/purchases/part-00000-5e91ed8b-6ee9-4317-87ee-1ffe29bfa8f5-c000.snappy.parquet
```

We can see that parquest and _SUCCESS files have been written to HDFS.


##### Create a pyspark script that will impose schema-on-read on the parquet files for spark SQL  #####

```bash
$ vim game_api.py
```

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_events = purchase_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_events.printSchema()
    extracted_purchase_events.show()

    extracted_purchase_events.registerTempTable("extracted_purchase_events")

    spark.sql("""
        create external table purchases
        stored as parquet
        location '/tmp/purchases'
        as
        select * from extracted_purchase_events
    """)


if __name__ == "__main__":
    main()
```

Create a file called write_hive_table.py and save the fiee to the file system after editing

##### Start up Hive #####


```bash
$ docker-compose exec spark pyspark
```

```
2018-04-13 23:21:41,338 WARN  [main] mapreduce.TableMapReduceUtil: The hbase-prefix-tree module jar containing PrefixTreeCodec is not present.  Continuing without it.

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> 
```

Hive is started up

##### Run python spark code using pyspark #####

```bash
$ docker-compose exec spark spark-submit /w205/full-stack2/write_hive_table.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/04/13 23:17:49 INFO SparkContext: Running Spark version 2.2.0
18/04/13 23:17:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/13 23:17:51 INFO SparkContext: Submitted application: ExtractEventsJob
18/04/13 23:17:51 INFO SecurityManager: Changing view acls to: root
18/04/13 23:17:51 INFO SecurityManager: Changing modify acls to: root
18/04/13 23:17:51 INFO SecurityManager: Changing view acls groups to: 
18/04/13 23:17:51 INFO SecurityManager: Changing modify acls groups to: 
18/04/13 23:17:51 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/04/13 23:17:52 INFO Utils: Successfully started service 'sparkDriver' on port 42012.
18/04/13 23:17:53 INFO SparkEnv: Registering MapOutputTracker
18/04/13 23:17:53 INFO SparkEnv: Registering BlockManagerMaster
18/04/13 23:17:53 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/04/13 23:17:53 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/04/13 23:17:53 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-4bd35f20-1bc5-4fad-9934-107029ef5bb3
18/04/13 23:17:53 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/04/13 23:17:53 INFO SparkEnv: Registering OutputCommitCoordinator
18/04/13 23:17:54 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/04/13 23:17:54 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.7:4040
18/04/13 23:17:55 INFO SparkContext: Added file file:/w205/activity-12-takoloco/write_hive_table.py at file:/w205/activity-12-takoloco/write_hive_table.py with timestamp 1523661475083
18/04/13 23:17:55 INFO Utils: Copying /w205/activity-12-takoloco/write_hive_table.py to /tmp/spark-8b7584e6-5c9e-435d-aa96-226552665e19/userFiles-02e40c01-55c4-4ab1-8612-248e9d46fa8c/write_hive_table.py
18/04/13 23:17:55 INFO Executor: Starting executor ID driver on host localhost
18/04/13 23:17:55 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34430.
18/04/13 23:17:55 INFO NettyBlockTransferService: Server created on 172.18.0.7:34430
18/04/13 23:17:55 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/04/13 23:17:55 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.7, 34430, None)
18/04/13 23:17:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.7:34430 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.7, 34430, None)
18/04/13 23:17:55 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.7, 34430, None)
18/04/13 23:17:55 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.7, 34430, None)
18/04/13 23:17:56 INFO SharedState: loading hive config file: file:/spark-2.2.0-bin-hadoop2.6/conf/hive-site.xml
18/04/13 23:17:56 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/04/13 23:17:56 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/04/13 23:17:58 INFO HiveUtils: Initializing HiveMetastoreConnection version 1.2.1 using Spark classes.
18/04/13 23:18:00 INFO metastore: Trying to connect to metastore with URI thrift://cloudera:9083
18/04/13 23:18:01 INFO metastore: Connected to metastore.
18/04/13 23:18:03 INFO SessionState: Created HDFS directory: /tmp/hive/root
18/04/13 23:18:03 INFO SessionState: Created local directory: /tmp/root
18/04/13 23:18:03 INFO SessionState: Created local directory: /tmp/21f24718-1a0c-478d-9ecb-839a058b69c3_resources
18/04/13 23:18:03 INFO SessionState: Created HDFS directory: /tmp/hive/root/21f24718-1a0c-478d-9ecb-839a058b69c3
18/04/13 23:18:03 INFO SessionState: Created local directory: /tmp/root/21f24718-1a0c-478d-9ecb-839a058b69c3
18/04/13 23:18:03 INFO SessionState: Created HDFS directory: /tmp/hive/root/21f24718-1a0c-478d-9ecb-839a058b69c3/_tmp_space.db
18/04/13 23:18:03 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/
18/04/13 23:18:04 INFO SessionState: Created local directory: /tmp/4cc173b1-3de1-47c4-baec-cb95943f32fe_resources
18/04/13 23:18:04 INFO SessionState: Created HDFS directory: /tmp/hive/root/4cc173b1-3de1-47c4-baec-cb95943f32fe
18/04/13 23:18:04 INFO SessionState: Created local directory: /tmp/root/4cc173b1-3de1-47c4-baec-cb95943f32fe
18/04/13 23:18:04 INFO SessionState: Created HDFS directory: /tmp/hive/root/4cc173b1-3de1-47c4-baec-cb95943f32fe/_tmp_space.db
18/04/13 23:18:04 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/
18/04/13 23:18:04 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/04/13 23:18:09 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:10 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:11 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 1
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = earliest

18/04/13 23:18:11 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-1
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 1
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = earliest

18/04/13 23:18:11 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:18:11 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:18:11 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0.
18/04/13 23:18:11 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0
18/04/13 23:18:11 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0
18/04/13 23:18:14 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0 with generation 1
18/04/13 23:18:14 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-driver-0
18/04/13 23:18:15 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/04/13 23:18:17 INFO CodeGenerator: Code generated in 700.947615 ms
18/04/13 23:18:17 INFO CodeGenerator: Code generated in 59.490494 ms
18/04/13 23:18:21 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/04/13 23:18:21 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/04/13 23:18:21 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/04/13 23:18:21 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:18:21 INFO DAGScheduler: Missing parents: List()
18/04/13 23:18:21 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/04/13 23:18:21 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 22.5 KB, free 366.3 MB)
18/04/13 23:18:21 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.8 KB, free 366.3 MB)
18/04/13 23:18:21 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.7:34430 (size: 10.8 KB, free: 366.3 MB)
18/04/13 23:18:21 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/04/13 23:18:21 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/04/13 23:18:21 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/04/13 23:18:22 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:18:22 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/04/13 23:18:22 INFO Executor: Fetching file:/w205/activity-12-takoloco/write_hive_table.py with timestamp 1523661475083
18/04/13 23:18:22 INFO Utils: /w205/activity-12-takoloco/write_hive_table.py has been previously copied to /tmp/spark-8b7584e6-5c9e-435d-aa96-226552665e19/userFiles-02e40c01-55c4-4ab1-8612-248e9d46fa8c/write_hive_table.py
18/04/13 23:18:22 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:22 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-2
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:22 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:18:22 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:18:22 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor.
18/04/13 23:18:22 INFO CodeGenerator: Code generated in 60.128059 ms
18/04/13 23:18:23 INFO CodeGenerator: Code generated in 119.6137 ms
18/04/13 23:18:23 INFO CodeGenerator: Code generated in 85.984453 ms
18/04/13 23:18:24 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:25 INFO CodeGenerator: Code generated in 75.116274 ms
18/04/13 23:18:25 INFO PythonRunner: Times: total = 1946, boot = 1545, init = 400, finish = 1
18/04/13 23:18:25 INFO PythonRunner: Times: total = 98, boot = 13, init = 84, finish = 1
18/04/13 23:18:25 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2093 bytes result sent to driver
18/04/13 23:18:25 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 3579 ms on localhost (executor driver) (1/1)
18/04/13 23:18:25 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/04/13 23:18:25 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 3.681 s
18/04/13 23:18:25 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 4.487665 s
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- timestamp: string (nullable = true)

18/04/13 23:18:26 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/04/13 23:18:26 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/13 23:18:26 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/04/13 23:18:26 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:18:26 INFO DAGScheduler: Missing parents: List()
18/04/13 23:18:26 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/13 23:18:26 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 27.6 KB, free 366.2 MB)
18/04/13 23:18:26 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 13.5 KB, free 366.2 MB)
18/04/13 23:18:26 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.7:34430 (size: 13.5 KB, free: 366.3 MB)
18/04/13 23:18:26 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/04/13 23:18:26 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/13 23:18:26 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/04/13 23:18:26 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:18:26 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/04/13 23:18:26 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:26 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-3
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:26 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:18:26 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:18:26 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor.
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:26 INFO PythonRunner: Times: total = 165, boot = -1259, init = 1422, finish = 2
18/04/13 23:18:26 INFO CodeGenerator: Code generated in 45.609484 ms
18/04/13 23:18:26 INFO PythonRunner: Times: total = 60, boot = 9, init = 38, finish = 13
18/04/13 23:18:26 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2247 bytes result sent to driver
18/04/13 23:18:26 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 543 ms on localhost (executor driver) (1/1)
18/04/13 23:18:26 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/04/13 23:18:26 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) finished in 0.550 s
18/04/13 23:18:26 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.661781 s
18/04/13 23:18:26 INFO CodeGenerator: Code generated in 63.49171 ms
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:17:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-13 19:20:...|
+------+-----------------+---------------+--------------+--------------------+
only showing top 20 rows

18/04/13 23:18:26 INFO SparkSqlParser: Parsing command: extracted_purchase_events
18/04/13 23:18:27 INFO SparkSqlParser: Parsing command: 
        create external table purchases
        stored as parquet
        location '/tmp/purchases'
        as
        select * from extracted_purchase_events
    
18/04/13 23:18:29 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:29 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:29 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:29 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:29 INFO CatalystSqlParser: Parsing command: string
18/04/13 23:18:30 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:18:30 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:18:30 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:18:30 INFO SparkContext: Starting job: sql at NativeMethodAccessorImpl.java:0
18/04/13 23:18:30 INFO DAGScheduler: Got job 2 (sql at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/13 23:18:30 INFO DAGScheduler: Final stage: ResultStage 2 (sql at NativeMethodAccessorImpl.java:0)
18/04/13 23:18:30 INFO DAGScheduler: Parents of final stage: List()
18/04/13 23:18:30 INFO DAGScheduler: Missing parents: List()
18/04/13 23:18:30 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[19] at sql at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/13 23:18:30 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 92.5 KB, free 366.1 MB)
18/04/13 23:18:30 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 36.5 KB, free 366.1 MB)
18/04/13 23:18:30 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.7:34430 (size: 36.5 KB, free: 366.2 MB)
18/04/13 23:18:30 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1006
18/04/13 23:18:30 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[19] at sql at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/13 23:18:30 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
18/04/13 23:18:30 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/13 23:18:30 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
18/04/13 23:18:30 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = 
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:30 INFO ConsumerConfig: ConsumerConfig values: 
	metric.reporters = []
	metadata.max.age.ms = 300000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
	reconnect.backoff.ms = 50
	sasl.kerberos.ticket.renew.window.factor = 0.8
	max.partition.fetch.bytes = 1048576
	bootstrap.servers = [kafka:29092]
	ssl.keystore.type = JKS
	enable.auto.commit = false
	sasl.mechanism = GSSAPI
	interceptor.classes = null
	exclude.internal.topics = true
	ssl.truststore.password = null
	client.id = consumer-4
	ssl.endpoint.identification.algorithm = null
	max.poll.records = 2147483647
	check.crcs = true
	request.timeout.ms = 40000
	heartbeat.interval.ms = 3000
	auto.commit.interval.ms = 5000
	receive.buffer.bytes = 65536
	ssl.truststore.type = JKS
	ssl.truststore.location = null
	ssl.keystore.password = null
	fetch.min.bytes = 1
	send.buffer.bytes = 131072
	value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	group.id = spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor
	retry.backoff.ms = 100
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	ssl.trustmanager.algorithm = PKIX
	ssl.key.password = null
	fetch.max.wait.ms = 500
	sasl.kerberos.min.time.before.relogin = 60000
	connections.max.idle.ms = 540000
	session.timeout.ms = 30000
	metrics.num.samples = 2
	key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
	ssl.protocol = TLS
	ssl.provider = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.keystore.location = null
	ssl.cipher.suites = null
	security.protocol = PLAINTEXT
	ssl.keymanager.algorithm = SunX509
	metrics.sample.window.ms = 30000
	auto.offset.reset = none

18/04/13 23:18:30 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/13 23:18:30 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/13 23:18:30 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-717bffe0-2f8a-4229-91aa-a91bf28230ef-executor.
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/13 23:18:30 INFO PythonRunner: Times: total = 113, boot = -3889, init = 4001, finish = 1
18/04/13 23:18:30 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:18:30 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/13 23:18:30 INFO CodecConfig: Compression: SNAPPY
18/04/13 23:18:30 INFO CodecConfig: Compression: SNAPPY
18/04/13 23:18:30 INFO ParquetOutputFormat: Parquet block size to 134217728
18/04/13 23:18:30 INFO ParquetOutputFormat: Parquet page size to 1048576
18/04/13 23:18:30 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
18/04/13 23:18:30 INFO ParquetOutputFormat: Dictionary is on
18/04/13 23:18:30 INFO ParquetOutputFormat: Validation is off
18/04/13 23:18:30 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
18/04/13 23:18:30 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
18/04/13 23:18:30 INFO ParquetOutputFormat: Page size checking is: estimated
18/04/13 23:18:30 INFO ParquetOutputFormat: Min row count for page size check is: 100
18/04/13 23:18:30 INFO ParquetOutputFormat: Max row count for page size check is: 10000
18/04/13 23:18:31 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "Accept",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Host",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "User-Agent",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "event_type",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "timestamp",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary Accept (UTF8);
  optional binary Host (UTF8);
  optional binary User-Agent (UTF8);
  optional binary event_type (UTF8);
  optional binary timestamp (UTF8);
}

       
18/04/13 23:18:31 INFO CodecPool: Got brand-new compressor [.snappy]
18/04/13 23:18:31 INFO PythonRunner: Times: total = 65, boot = -3969, init = 4031, finish = 3
18/04/13 23:18:31 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 1489
18/04/13 23:18:33 INFO FileOutputCommitter: Saved output of task 'attempt_20180413231830_0002_m_000000_0' to hdfs://cloudera/tmp/purchases/_temporary/0/task_20180413231830_0002_m_000000
18/04/13 23:18:33 INFO SparkHadoopMapRedUtil: attempt_20180413231830_0002_m_000000_0: Committed
18/04/13 23:18:33 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2325 bytes result sent to driver
18/04/13 23:18:33 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 2861 ms on localhost (executor driver) (1/1)
18/04/13 23:18:33 INFO DAGScheduler: ResultStage 2 (sql at NativeMethodAccessorImpl.java:0) finished in 2.873 s
18/04/13 23:18:33 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/04/13 23:18:33 INFO DAGScheduler: Job 2 finished: sql at NativeMethodAccessorImpl.java:0, took 2.971582 s
18/04/13 23:18:33 INFO FileFormatWriter: Job null committed.
18/04/13 23:18:34 INFO SparkContext: Invoking stop() from shutdown hook
18/04/13 23:18:34 INFO SparkUI: Stopped Spark web UI at http://172.18.0.7:4040
18/04/13 23:18:34 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/04/13 23:18:34 INFO MemoryStore: MemoryStore cleared
18/04/13 23:18:34 INFO BlockManager: BlockManager stopped
18/04/13 23:18:34 INFO BlockManagerMaster: BlockManagerMaster stopped
18/04/13 23:18:34 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/04/13 23:18:34 INFO SparkContext: Successfully stopped SparkContext
18/04/13 23:18:34 INFO ShutdownHookManager: Shutdown hook called
18/04/13 23:18:34 INFO ShutdownHookManager: Deleting directory /tmp/spark-8b7584e6-5c9e-435d-aa96-226552665e19
18/04/13 23:18:34 INFO ShutdownHookManager: Deleting directory /tmp/spark-8b7584e6-5c9e-435d-aa96-226552665e19/pyspark-7827885a-9ca4-4c68-ab47-fe16ed4de787
```

The script extracts all purchase events and prints them out as well as its schema and stores them in an external table purchases.


##### Confirm that the data has been stored in purchases table using hive #####

###### Table list ######

```
hive> show tables;
```

```
OK
purchases
Time taken: 2.699 seconds, Fetched: 1 row(s)
```

###### Schema of purchases talbe ######

```
hive> desc purchases;
```

```
OK
accept              	string              	                    
host                	string              	                    
user-agent          	string              	                    
event_type          	string              	                    
timestamp           	string              	                    
Time taken: 0.72 seconds, Fetched: 5 row(s)
```

###### Query purchases table ######

```
hive> SELECT * FROM purchases;
```

```
OK
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/jars/parquet-hadoop-bundle-1.5.0-cdh5.7.0.jar!/shaded/parquet/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/parquet-format-2.1.0-cdh5.7.0.jar!/shaded/parquet/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/parquet-pig-bundle-1.5.0-cdh5.7.0.jar!/shaded/parquet/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/hive-exec-1.1.0-cdh5.7.0.jar!/shaded/parquet/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/hive-jdbc-1.1.0-cdh5.7.0-standalone.jar!/shaded/parquet/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [shaded.parquet.org.slf4j.helpers.NOPLoggerFactory]
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:50.97
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:50.994
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.019
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.029
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.032
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.043
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.054
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.065
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.08
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:17:51.095
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.462
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.476
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.488
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.512
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.536
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.538
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.548
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.57
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.589
*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:20:16.596
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.235
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.265
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.292
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.295
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.297
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.312
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.315
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.317
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.319
*/*	user2.att.com	ApacheBench/2.3	purchase_sword	2018-04-13 19:30:37.323
Time taken: 1.719 seconds, Fetched: 30 row(s)
```

Table list, schema and SELECT query result are output via hive successfully.

##### Verify that the files have been written to HDFS #####

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/
```

```
Found 4 items
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2018-04-13 23:18 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2018-04-13 23:18 /tmp/purchases
```

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases/
```

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-04-13 23:18 /tmp/purchases/_SUCCESS
-rw-r--r--   1 root supergroup       1724 2018-04-13 23:18 /tmp/purchases/part-00000-0a4ec865-101f-47d6-b750-e776855a46e4-c000.snappy.parquet
```

We can see that the files are persisting in HDFS.


##### Start up Presto #####

```bash
$ docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```

```
presto:default> 
```

Presto starts up and returns user prompt

##### Play around with Presto #####

###### Table list ######

```sql
SHOW TABLES;
```

```
   Table   
-----------
 purchases 
(1 row)

Query 20180413_233629_00002_ctux8, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:02 [1 rows, 34B] [0 rows/s, 18B/s]
```

###### Schema of purchases table ######

```sql
DESC purchases;
```

```
   Column   |  Type   | Comment 
------------+---------+---------
 accept     | varchar |         
 host       | varchar |         
 user-agent | varchar |         
 event_type | varchar |         
 timestamp  | varchar |         
(5 rows)

Query 20180413_233738_00003_ctux8, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:02 [5 rows, 344B] [2 rows/s, 195B/s]
```

###### Query purchases table ######

```sql
SELECT * FROM purchases;
```

```
 accept |       host        |   user-agent    |   event_type   |        timestamp        
--------+-------------------+-----------------+----------------+-------------------------
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:50.97  
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:50.994 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.019 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.029 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.032 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.043 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.054 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.065 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.08  
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:17:51.095 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.462 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.476 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.488 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.512 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.536 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.538 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.548 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.57  
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.589 
 */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:20:16.596 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.235 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.265 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.292 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.295 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.297 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.312 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.315 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.317 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.319 
 */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword | 2018-04-13 19:30:37.323 
(30 rows)

Query 20180413_233820_00004_ctux8, FINISHED, 1 node
Splits: 2 total, 2 done (100.00%)
0:08 [30 rows, 1.68KB] [3 rows/s, 213B/s]
```

Table list, schema and SELECT query result are output via presto successfully.

##### Terminate running processes #####

###### Presto ######

```
presto:default> exit
$
```

###### Hive ######

```
 exit;
WARN: The method class org.apache.commons.logging.impl.SLF4JLogFactory#release() was invoked.
WARN: Please see http://www.slf4j.org/codes.html#release for an explanation.
Apr 13, 2018 11:47:35 PM WARNING: parquet.hadoop.ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
Apr 13, 2018 11:47:36 PM INFO: parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 30 records.
Apr 13, 2018 11:47:36 PM INFO: parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Apr 13, 2018 11:47:36 PM INFO: parquet.hadoop.InternalParquetRecordReader: block read in memory in 38 ms. row count = 30
```

###### Kafkacat ######

```bash
^c
$
```

###### Python web server ######

```bash
^c
$
```

##### Take down the cluster #####

```bash
$ docker-compose down
```

```
Stopping activity12takoloco_spark_1 ... done
Stopping activity12takoloco_kafka_1 ... done
Stopping activity12takoloco_zookeeper_1 ... done
Stopping activity12takoloco_cloudera_1 ... done
Stopping activity12takoloco_mids_1 ... done
Stopping activity12takoloco_presto_1 ... done
Removing activity12takoloco_spark_1 ... done
Removing activity12takoloco_kafka_1 ... done
Removing activity12takoloco_zookeeper_1 ... done
Removing activity12takoloco_cloudera_1 ... done
Removing activity12takoloco_mids_1 ... done
Removing activity12takoloco_presto_1 ... done
Removing network activity12takoloco_default
```

##### Confirm all containers have been properly shut down #####

```bash
$ docker-compose ps
```

```
Name   Command   State   Ports 
------------------------------
```

```bash
$ docker ps -a
```

```
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
```

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

# Assignment 11

## Follow the steps we did in class 


### Turn in your `/assignment-11-<user-name>/README.md` file. It should include:
#### 1) A summary type explanation of the example. ####
* Copy docker-compose.yaml and Python scripts from the course-content directory
* Modify docker-compose.yaml to use the absolute path /home/science/w205 instead of ~/w205
* Start up the docker cluster
* Create a topic on Kafka
* Create a python web server script using the flask library which publishes events to Kafka topic
* Start up the web server
* Start up kafkacat in the continous mode
* Access the web server via the Apache Bench command
* Create a python script for extracting and separating events using Spark
* Execute separate_events.py to extract events from kafka via spark-submit
* Create a python script for extracting events from kafka and filtering them for purchase events only using Spark
* Stop the web server script game_api.py with Ctrl-C
* Edit the python web server script game_api.py and add the below handler for handling the event "purchase_knife"
* Restart the web server script game_api.py
* Create a python script for extracting events from kafka and filtering them for purchase events only and writing them out to HDFS using Spark
* Execute filtered_writes.py to extract events from kafka via spark-submit
* Verify that the files have been written to HDFS
* Start up Jupyter Notebook
* Create a new Jupyter Notebook
* Get the path to the current directory in the notebook
* Run bash in the Spark package context
* Create a symlink to the volume /w205
* Create a new Jupyter Notebook in the directory activity-11-takoloco under the volume /w205
* Stop the web server, kafkacat and Jupyter Notebook with a control-C
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
Creating network "activity11takoloco_default" with the default driver
Creating activity11takoloco_zookeeper_1
Creating activity11takoloco_cloudera_1
Creating activity11takoloco_mids_1
Creating activity11takoloco_kafka_1
Creating activity11takoloco_spark_1
```

Start up the Docker cluster defined in the docker-compose.yaml which include the following services:

* zookeeper
* kafka
* cloudera
* spark
* mids

##### Create a topic on Kafka #####

```bash
$ docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```
```
Created topic "events".
```

Create a Kafka topic called "events"

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
$ docker-compose exec mids env FLASK_APP=/w205/activity-11-takoloco/game_api.py flask run --host 0.0.0.0
```

```
 * Serving Flask app "game_api"
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Start up a Flask web server process with the Python file /w205/activity-11-takoloco/game_api.py

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
Time taken for tests:   0.046 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1850 bytes
HTML transferred:       300 bytes
Requests per second:    217.10 [#/sec] (mean)
Time per request:       4.606 [ms] (mean)
Time per request:       4.606 [ms] (mean, across all concurrent requests)
Transfer rate:          39.22 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     2    4   2.8      4      12
Waiting:        2    4   3.0      3      11
Total:          3    4   2.8      4      12

Percentage of the requests served within a certain time (ms)
  50%      4
  66%      4
  75%      4
  80%      7
  90%     12
  95%     12
  98%     12
  99%     12
 100%     12 (longest request)
```

###### Server ######

```
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:25:43] "GET / HTTP/1.0" 200 -
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
Time taken for tests:   0.058 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1720 bytes
HTML transferred:       170 bytes
Requests per second:    173.82 [#/sec] (mean)
Time per request:       5.753 [ms] (mean)
Time per request:       5.753 [ms] (mean, across all concurrent requests)
Transfer rate:          29.20 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.2      0       1
Processing:     2    5   3.5      4      14
Waiting:        2    5   3.5      4      14
Total:          2    6   3.5      5      14

Percentage of the requests served within a certain time (ms)
  50%      5
  66%      6
  75%      7
  80%      7
  90%     14
  95%     14
  98%     14
  99%     14
 100%     14 (longest request)
```

###### Server ######

```
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:13] "GET /purchase_a_sword HTTP/1.0" 200 -
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
Time taken for tests:   0.067 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1850 bytes
HTML transferred:       300 bytes
Requests per second:    149.02 [#/sec] (mean)
Time per request:       6.711 [ms] (mean)
Time per request:       6.711 [ms] (mean, across all concurrent requests)
Transfer rate:          26.92 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     3    6   3.6      6      14
Waiting:        2    4   2.3      3       8
Total:          3    6   3.6      6      14

Percentage of the requests served within a certain time (ms)
  50%      6
  66%      7
  75%      7
  80%     10
  90%     14
  95%     14
  98%     14
  99%     14
 100%     14 (longest request)
```

###### Server ######

```
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:28:49] "GET / HTTP/1.0" 200 -
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
Time taken for tests:   0.054 seconds
Complete requests:      10
Failed requests:        0
Total transferred:      1720 bytes
HTML transferred:       170 bytes
Requests per second:    185.25 [#/sec] (mean)
Time per request:       5.398 [ms] (mean)
Time per request:       5.398 [ms] (mean, across all concurrent requests)
Transfer rate:          31.12 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.1      0       0
Processing:     2    5   2.7      4      10
Waiting:        0    2   1.4      3       4
Total:          2    5   2.7      5      10

Percentage of the requests served within a certain time (ms)
  50%      5
  66%      5
  75%      7
  80%      9
  90%     10
  95%     10
  98%     10
  99%     10
 100%     10 (longest request)
```

###### Server ######

```
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
127.0.0.1 - - [06/Apr/2018 05:29:28] "GET /purchase_a_sword HTTP/1.0" 200 -
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

##### Create a python script for extracting and separating events using Spark #####

```bash
$ vim separate_events.py
```

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Host'] = "moe"
    event['Cache-Control'] = "no-cache"
    return json.dumps(event)


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

    munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_events = munged_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()1G

    sword_purchases = extracted_events \
        .filter(extracted_events.event_type == 'purchase_sword')
    sword_purchases.show()
    # sword_purchases \
        # .write \
        # .mode("overwrite") \
        # .parquet("/tmp/sword_purchases")

    default_hits = extracted_events \
        .filter(extracted_events.event_type == 'default')
    default_hits.show()
    # default_hits \
        # .write \
        # .mode("overwrite") \
        # .parquet("/tmp/default_hits")


if __name__ == "__main__":
    main()
```

Create a file called separate_events.py and save the file to the file system after editing

##### Execute separate_events.py to extract events from kafka via spark-submit #####

```bash
$ docker-compose exec spark spark-submit /w205/spark-from-files/separate_events.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/04/06 12:21:48 INFO SparkContext: Running Spark version 2.2.0
18/04/06 12:21:49 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/06 12:21:49 INFO SparkContext: Submitted application: ExtractEventsJob
18/04/06 12:21:49 INFO SecurityManager: Changing view acls to: root
18/04/06 12:21:49 INFO SecurityManager: Changing modify acls to: root
18/04/06 12:21:49 INFO SecurityManager: Changing view acls groups to: 
18/04/06 12:21:49 INFO SecurityManager: Changing modify acls groups to: 
18/04/06 12:21:49 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/04/06 12:21:50 INFO Utils: Successfully started service 'sparkDriver' on port 41666.
18/04/06 12:21:50 INFO SparkEnv: Registering MapOutputTracker
18/04/06 12:21:50 INFO SparkEnv: Registering BlockManagerMaster
18/04/06 12:21:50 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/04/06 12:21:50 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/04/06 12:21:51 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-e0245b63-cdea-4402-afaa-10e945892b15
18/04/06 12:21:51 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/04/06 12:21:51 INFO SparkEnv: Registering OutputCommitCoordinator
18/04/06 12:21:52 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/04/06 12:21:52 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.6:4040
18/04/06 12:21:52 INFO SparkContext: Added file file:/w205/activity-11-takoloco/separate_events.py at file:/w205/activity-11-takoloco/separate_events.py with timestamp 1523017312536
18/04/06 12:21:52 INFO Utils: Copying /w205/activity-11-takoloco/separate_events.py to /tmp/spark-320c94f0-578c-4853-b04f-220e6daeb9d3/userFiles-ac6ef23a-8f23-43ca-86da-695452694889/separate_events.py
18/04/06 12:21:52 INFO Executor: Starting executor ID driver on host localhost
18/04/06 12:21:52 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 40874.
18/04/06 12:21:52 INFO NettyBlockTransferService: Server created on 172.18.0.6:40874
18/04/06 12:21:52 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/04/06 12:21:52 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.6, 40874, None)
18/04/06 12:21:52 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:40874 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.6, 40874, None)
18/04/06 12:21:52 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.6, 40874, None)
18/04/06 12:21:52 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.6, 40874, None)
18/04/06 12:21:53 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/04/06 12:21:53 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/04/06 12:21:55 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/04/06 12:21:59 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:21:59 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:22:00 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0
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

18/04/06 12:22:00 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0
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

18/04/06 12:22:00 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:22:00 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:22:01 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0.
18/04/06 12:22:01 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0
18/04/06 12:22:01 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0
18/04/06 12:22:04 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0 with generation 1
18/04/06 12:22:04 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-driver-0
18/04/06 12:22:04 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/04/06 12:22:06 INFO CodeGenerator: Code generated in 789.398373 ms
18/04/06 12:22:06 INFO CodeGenerator: Code generated in 43.568742 ms
18/04/06 12:22:09 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/04/06 12:22:09 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/04/06 12:22:09 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/04/06 12:22:09 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:22:09 INFO DAGScheduler: Missing parents: List()
18/04/06 12:22:09 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/04/06 12:22:09 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 21.4 KB, free 366.3 MB)
18/04/06 12:22:10 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.5 KB, free 366.3 MB)
18/04/06 12:22:10 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:40874 (size: 10.5 KB, free: 366.3 MB)
18/04/06 12:22:10 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/04/06 12:22:10 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/04/06 12:22:10 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/04/06 12:22:10 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:22:10 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/04/06 12:22:10 INFO Executor: Fetching file:/w205/activity-11-takoloco/separate_events.py with timestamp 1523017312536
18/04/06 12:22:10 INFO Utils: /w205/activity-11-takoloco/separate_events.py has been previously copied to /tmp/spark-320c94f0-578c-4853-b04f-220e6daeb9d3/userFiles-ac6ef23a-8f23-43ca-86da-695452694889/separate_events.py
18/04/06 12:22:10 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor
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

18/04/06 12:22:10 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor
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

18/04/06 12:22:10 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:22:10 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:22:10 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor.
18/04/06 12:22:11 INFO CodeGenerator: Code generated in 96.556932 ms
18/04/06 12:22:11 INFO CodeGenerator: Code generated in 160.732897 ms
18/04/06 12:22:11 INFO CodeGenerator: Code generated in 24.724932 ms
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:12 INFO CodeGenerator: Code generated in 80.090171 ms
18/04/06 12:22:12 INFO PythonRunner: Times: total = 1341, boot = 1184, init = 153, finish = 4
18/04/06 12:22:12 INFO PythonRunner: Times: total = 85, boot = 8, init = 76, finish = 1
18/04/06 12:22:13 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2003 bytes result sent to driver
18/04/06 12:22:13 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 2763 ms on localhost (executor driver) (1/1)
18/04/06 12:22:13 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/04/06 12:22:13 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 2.839 s
18/04/06 12:22:13 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 3.899519 s
18/04/06 12:22:14 INFO CodeGenerator: Code generated in 32.409857 ms
18/04/06 12:22:14 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/04/06 12:22:14 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/06 12:22:14 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/04/06 12:22:14 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:22:14 INFO DAGScheduler: Missing parents: List()
18/04/06 12:22:15 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[19] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/06 12:22:15 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 30.3 KB, free 366.2 MB)
18/04/06 12:22:15 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 14.2 KB, free 366.2 MB)
18/04/06 12:22:15 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.6:40874 (size: 14.2 KB, free: 366.3 MB)
18/04/06 12:22:15 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/04/06 12:22:15 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[19] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/06 12:22:15 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/04/06 12:22:15 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:22:15 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/04/06 12:22:15 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor
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

18/04/06 12:22:15 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor
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

18/04/06 12:22:15 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:22:15 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:22:15 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:40874 in memory (size: 10.5 KB, free: 366.3 MB)
18/04/06 12:22:15 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-886eac26-f0d8-4b93-b5ac-1cd1b9822395-executor.
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:22:15 INFO PythonRunner: Times: total = 179, boot = -2557, init = 2735, finish = 1
18/04/06 12:22:15 INFO CodeGenerator: Code generated in 68.096318 ms
18/04/06 12:22:15 INFO PythonRunner: Times: total = 113, boot = 40, init = 44, finish = 29
18/04/06 12:22:15 ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 1)
java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 6 fields are required while 8 values are provided.
	at org.apache.spark.sql.execution.python.EvaluatePython$.fromJava(EvaluatePython.scala:138)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:234)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:228)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:108)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
18/04/06 12:22:15 WARN TaskSetManager: Lost task 0.0 in stage 1.0 (TID 1, localhost, executor driver): java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 6 fields are required while 8 values are provided.
	at org.apache.spark.sql.execution.python.EvaluatePython$.fromJava(EvaluatePython.scala:138)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:234)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:228)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:108)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)

18/04/06 12:22:15 ERROR TaskSetManager: Task 0 in stage 1.0 failed 1 times; aborting job
18/04/06 12:22:15 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/04/06 12:22:15 INFO TaskSchedulerImpl: Cancelling stage 1
18/04/06 12:22:15 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) failed in 0.731 s due to Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 1, localhost, executor driver): java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 6 fields are required while 8 values are provided.
	at org.apache.spark.sql.execution.python.EvaluatePython$.fromJava(EvaluatePython.scala:138)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:234)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:228)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:108)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)

Driver stacktrace:
18/04/06 12:22:15 INFO DAGScheduler: Job 1 failed: showString at NativeMethodAccessorImpl.java:0, took 0.909335 s
Traceback (most recent call last):
  File "/w205/activity-11-takoloco/separate_events.py", line 62, in <module>
    main()
  File "/w205/activity-11-takoloco/separate_events.py", line 46, in main
    sword_purchases.show()
  File "/spark-2.2.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 336, in show
  File "/spark-2.2.0-bin-hadoop2.6/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py", line 1133, in __call__
  File "/spark-2.2.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
  File "/spark-2.2.0-bin-hadoop2.6/python/lib/py4j-0.10.4-src.zip/py4j/protocol.py", line 319, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o80.showString.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 1, localhost, executor driver): java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 6 fields are required while 8 values are provided.
	at org.apache.spark.sql.execution.python.EvaluatePython$.fromJava(EvaluatePython.scala:138)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:234)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:228)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:108)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1499)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1487)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1486)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1486)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:814)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:814)
	at scala.Option.foreach(Option.scala:257)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:814)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:1714)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1669)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1658)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:630)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2022)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2043)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2062)
	at org.apache.spark.sql.execution.SparkPlan.executeTake(SparkPlan.scala:336)
	at org.apache.spark.sql.execution.CollectLimitExec.executeCollect(limit.scala:38)
	at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$collectFromPlan(Dataset.scala:2853)
	at org.apache.spark.sql.Dataset$$anonfun$head$1.apply(Dataset.scala:2153)
	at org.apache.spark.sql.Dataset$$anonfun$head$1.apply(Dataset.scala:2153)
	at org.apache.spark.sql.Dataset$$anonfun$55.apply(Dataset.scala:2837)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)
	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:2836)
	at org.apache.spark.sql.Dataset.head(Dataset.scala:2153)
	at org.apache.spark.sql.Dataset.take(Dataset.scala:2366)
	at org.apache.spark.sql.Dataset.showString(Dataset.scala:245)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:280)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:214)
	at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 6 fields are required while 8 values are provided.
	at org.apache.spark.sql.execution.python.EvaluatePython$.fromJava(EvaluatePython.scala:138)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at org.apache.spark.sql.SparkSession$$anonfun$5.apply(SparkSession.scala:728)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:409)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:234)
	at org.apache.spark.sql.execution.SparkPlan$$anonfun$2.apply(SparkPlan.scala:228)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.RDD$$anonfun$mapPartitionsInternal$1$$anonfun$apply$25.apply(RDD.scala:827)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:108)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:335)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	... 1 more

18/04/06 12:22:16 INFO SparkContext: Invoking stop() from shutdown hook
18/04/06 12:22:16 INFO SparkUI: Stopped Spark web UI at http://172.18.0.6:4040
18/04/06 12:22:16 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/04/06 12:22:16 INFO MemoryStore: MemoryStore cleared
18/04/06 12:22:16 INFO BlockManager: BlockManager stopped
18/04/06 12:22:16 INFO BlockManagerMaster: BlockManagerMaster stopped
18/04/06 12:22:16 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/04/06 12:22:16 INFO SparkContext: Successfully stopped SparkContext
18/04/06 12:22:16 INFO ShutdownHookManager: Shutdown hook called
18/04/06 12:22:16 INFO ShutdownHookManager: Deleting directory /tmp/spark-320c94f0-578c-4853-b04f-220e6daeb9d3
18/04/06 12:22:16 INFO ShutdownHookManager: Deleting directory /tmp/spark-320c94f0-578c-4853-b04f-220e6daeb9d3/pyspark-5bbdbda3-0bed-4694-983f-5697313fd248
```

Script breaks as the schema is expecting 6 values but 8 values are provided.

##### Create a python script for extracting events from kafka and filtering them for purchase events only using Spark #####

```bash
$ vim ~/w205/full-stack/just_filtering.py 
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


if __name__ == "__main__":
    main()
```

Create a file called just_filtering.py and save the file to the file system after editing


```bash
$ docker-compose exec spark spark-submit /w205/full-stack/just_filtering.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/04/06 12:29:47 INFO SparkContext: Running Spark version 2.2.0
18/04/06 12:29:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/06 12:29:48 INFO SparkContext: Submitted application: ExtractEventsJob
18/04/06 12:29:48 INFO SecurityManager: Changing view acls to: root
18/04/06 12:29:48 INFO SecurityManager: Changing modify acls to: root
18/04/06 12:29:48 INFO SecurityManager: Changing view acls groups to: 
18/04/06 12:29:48 INFO SecurityManager: Changing modify acls groups to: 
18/04/06 12:29:48 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/04/06 12:29:48 INFO Utils: Successfully started service 'sparkDriver' on port 38162.
18/04/06 12:29:48 INFO SparkEnv: Registering MapOutputTracker
18/04/06 12:29:49 INFO SparkEnv: Registering BlockManagerMaster
18/04/06 12:29:49 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/04/06 12:29:49 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/04/06 12:29:49 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-a3819dba-6e3b-42c4-ab5d-bd88b60b139d
18/04/06 12:29:49 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/04/06 12:29:49 INFO SparkEnv: Registering OutputCommitCoordinator
18/04/06 12:29:49 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/04/06 12:29:49 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.6:4040
18/04/06 12:29:49 INFO SparkContext: Added file file:/w205/full-stack/just_filtering.py at file:/w205/full-stack/just_filtering.py with timestamp 1523017789958
18/04/06 12:29:49 INFO Utils: Copying /w205/full-stack/just_filtering.py to /tmp/spark-68916ee6-03f5-40b6-830a-3ec150966dbb/userFiles-a3b3b5c2-5d88-4643-ab52-5c7202a4a966/just_filtering.py
18/04/06 12:29:50 INFO Executor: Starting executor ID driver on host localhost
18/04/06 12:29:50 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 42186.
18/04/06 12:29:50 INFO NettyBlockTransferService: Server created on 172.18.0.6:42186
18/04/06 12:29:50 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/04/06 12:29:50 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.6, 42186, None)
18/04/06 12:29:50 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:42186 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.6, 42186, None)
18/04/06 12:29:50 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.6, 42186, None)
18/04/06 12:29:50 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.6, 42186, None)
18/04/06 12:29:50 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/04/06 12:29:50 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/04/06 12:29:52 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/04/06 12:29:54 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:29:55 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:29:55 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0
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

18/04/06 12:29:55 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0
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

18/04/06 12:29:55 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:29:55 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:29:55 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0.
18/04/06 12:29:55 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0
18/04/06 12:29:55 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0
18/04/06 12:29:58 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0 with generation 1
18/04/06 12:29:58 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-driver-0
18/04/06 12:29:59 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/04/06 12:30:00 INFO CodeGenerator: Code generated in 380.49896 ms
18/04/06 12:30:00 INFO CodeGenerator: Code generated in 57.369216 ms
18/04/06 12:30:02 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/04/06 12:30:02 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/04/06 12:30:02 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/04/06 12:30:02 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:30:02 INFO DAGScheduler: Missing parents: List()
18/04/06 12:30:02 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/04/06 12:30:02 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 22.5 KB, free 366.3 MB)
18/04/06 12:30:02 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.8 KB, free 366.3 MB)
18/04/06 12:30:02 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:42186 (size: 10.8 KB, free: 366.3 MB)
18/04/06 12:30:02 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/04/06 12:30:02 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/04/06 12:30:02 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/04/06 12:30:02 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:30:02 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/04/06 12:30:02 INFO Executor: Fetching file:/w205/full-stack/just_filtering.py with timestamp 1523017789958
18/04/06 12:30:03 INFO Utils: /w205/full-stack/just_filtering.py has been previously copied to /tmp/spark-68916ee6-03f5-40b6-830a-3ec150966dbb/userFiles-a3b3b5c2-5d88-4643-ab52-5c7202a4a966/just_filtering.py
18/04/06 12:30:03 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor
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

18/04/06 12:30:03 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor
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

18/04/06 12:30:03 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:30:03 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:30:03 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor.
18/04/06 12:30:03 INFO CodeGenerator: Code generated in 33.678565 ms
18/04/06 12:30:03 INFO CodeGenerator: Code generated in 37.724631 ms
18/04/06 12:30:03 INFO CodeGenerator: Code generated in 45.658077 ms
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:04 INFO CodeGenerator: Code generated in 36.823586 ms
18/04/06 12:30:04 INFO PythonRunner: Times: total = 1093, boot = 929, init = 163, finish = 1
18/04/06 12:30:05 INFO PythonRunner: Times: total = 50, boot = 6, init = 43, finish = 1
18/04/06 12:30:05 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2049 bytes result sent to driver
18/04/06 12:30:05 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 2176 ms on localhost (executor driver) (1/1)
18/04/06 12:30:05 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/04/06 12:30:05 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 2.287 s
18/04/06 12:30:05 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 2.724600 s
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- timestamp: string (nullable = true)

18/04/06 12:30:05 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/04/06 12:30:05 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/06 12:30:05 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/04/06 12:30:05 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:30:05 INFO DAGScheduler: Missing parents: List()
18/04/06 12:30:05 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/06 12:30:05 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 27.5 KB, free 366.2 MB)
18/04/06 12:30:05 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 13.5 KB, free 366.2 MB)
18/04/06 12:30:05 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.6:42186 (size: 13.5 KB, free: 366.3 MB)
18/04/06 12:30:05 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/04/06 12:30:05 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/06 12:30:05 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/04/06 12:30:05 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:30:05 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/04/06 12:30:05 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor
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

18/04/06 12:30:05 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor
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

18/04/06 12:30:05 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:30:05 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:30:05 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-35ab83ce-1728-4587-9c94-60b6b2181f17-executor.
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:30:05 INFO PythonRunner: Times: total = 44, boot = -915, init = 958, finish = 1
18/04/06 12:30:05 INFO CodeGenerator: Code generated in 54.826855 ms
18/04/06 12:30:05 INFO PythonRunner: Times: total = 34, boot = 11, init = 21, finish = 2
18/04/06 12:30:06 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2214 bytes result sent to driver
18/04/06 12:30:06 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 373 ms on localhost (executor driver) (1/1)
18/04/06 12:30:06 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) finished in 0.393 s
18/04/06 12:30:06 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/04/06 12:30:06 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.562196 s
18/04/06 12:30:06 INFO CodeGenerator: Code generated in 34.290204 ms
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
+------+-----------------+---------------+--------------+--------------------+

18/04/06 12:30:06 INFO ContextCleaner: Cleaned accumulator 54
18/04/06 12:30:06 INFO SparkContext: Invoking stop() from shutdown hook
18/04/06 12:30:06 INFO SparkUI: Stopped Spark web UI at http://172.18.0.6:4040
18/04/06 12:30:06 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:42186 in memory (size: 10.8 KB, free: 366.3 MB)
18/04/06 12:30:06 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/04/06 12:30:06 INFO MemoryStore: MemoryStore cleared
18/04/06 12:30:06 INFO BlockManager: BlockManager stopped
18/04/06 12:30:06 INFO BlockManagerMaster: BlockManagerMaster stopped
18/04/06 12:30:06 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/04/06 12:30:06 INFO SparkContext: Successfully stopped SparkContext
18/04/06 12:30:06 INFO ShutdownHookManager: Shutdown hook called
18/04/06 12:30:06 INFO ShutdownHookManager: Deleting directory /tmp/spark-68916ee6-03f5-40b6-830a-3ec150966dbb/pyspark-7223311b-5f61-4d23-84da-17bc674fa236
18/04/06 12:30:06 INFO ShutdownHookManager: Deleting directory /tmp/spark-68916ee6-03f5-40b6-830a-3ec150966dbb
```

The script extracts all purchase events and prints them out as well as its schema.


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
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
+------+-----------------+---------------+--------------+--------------------+
```

##### Stop the web server script game_api.py with Ctrl-C #####

```bash
^C$
```

The web server script game_api.py stops running

##### Edit the python web server script game_api.py and add the below handler for handling the event "purchase_knife" #####

```bash
$ vim game_api.py
```

```python
@app.route("/purchase_a_knife")
def purchase_a_knife():
    purchase_knife_event = {'event_type': 'purchase_knife',
                            'description': 'very sharp knife'}
    log_to_kafka('events', purchase_knife_event)
    return "Knife Purchased!\n"
```

Saves the updated file game_api.py to the file system after editing

##### Restart the web server script game_api.py #####

```bash
$ docker-compose exec mids env FLASK_APP=/w205/activity-11-takoloco/game_api.py flask run --host 0.0.0.0
```

```
 * Serving Flask app "game_api"
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

The web server script game_api.py starts running again

##### Create a python script for extracting events from kafka and filtering them for purchase events only and writing them out to HDFS using Spark #####

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

Create a file called filtered_writes.py and save the file to the file system after editing

##### Execute filtered_writes.py to extract events from kafka via spark-submit #####

```bash
$ docker-compose exec spark spark-submit /w205/full-stack/filtered_writes.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/04/06 12:56:17 INFO SparkContext: Running Spark version 2.2.0
18/04/06 12:56:18 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/06 12:56:18 INFO SparkContext: Submitted application: ExtractEventsJob
18/04/06 12:56:18 INFO SecurityManager: Changing view acls to: root
18/04/06 12:56:18 INFO SecurityManager: Changing modify acls to: root
18/04/06 12:56:18 INFO SecurityManager: Changing view acls groups to: 
18/04/06 12:56:18 INFO SecurityManager: Changing modify acls groups to: 
18/04/06 12:56:18 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/04/06 12:56:19 INFO Utils: Successfully started service 'sparkDriver' on port 33422.
18/04/06 12:56:19 INFO SparkEnv: Registering MapOutputTracker
18/04/06 12:56:19 INFO SparkEnv: Registering BlockManagerMaster
18/04/06 12:56:19 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/04/06 12:56:19 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/04/06 12:56:19 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-2ac207b8-d262-41f7-a174-df6a04c680b8
18/04/06 12:56:19 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/04/06 12:56:19 INFO SparkEnv: Registering OutputCommitCoordinator
18/04/06 12:56:20 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/04/06 12:56:20 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.6:4040
18/04/06 12:56:20 INFO SparkContext: Added file file:/w205/full-stack/filtered_writes.py at file:/w205/full-stack/filtered_writes.py with timestamp 1523019380607
18/04/06 12:56:20 INFO Utils: Copying /w205/full-stack/filtered_writes.py to /tmp/spark-d2418eaf-1129-4cb6-9290-7e5371b57111/userFiles-10d1a31a-72b2-46b9-8ba1-482583985513/filtered_writes.py
18/04/06 12:56:20 INFO Executor: Starting executor ID driver on host localhost
18/04/06 12:56:20 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 41022.
18/04/06 12:56:20 INFO NettyBlockTransferService: Server created on 172.18.0.6:41022
18/04/06 12:56:20 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/04/06 12:56:20 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.6, 41022, None)
18/04/06 12:56:20 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:41022 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.6, 41022, None)
18/04/06 12:56:20 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.6, 41022, None)
18/04/06 12:56:20 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.6, 41022, None)
18/04/06 12:56:21 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/04/06 12:56:21 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/04/06 12:56:23 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/04/06 12:56:26 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:56:26 INFO CatalystSqlParser: Parsing command: string
18/04/06 12:56:27 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0
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

18/04/06 12:56:27 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0
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

18/04/06 12:56:27 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:56:27 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:56:27 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0.
18/04/06 12:56:27 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0
18/04/06 12:56:27 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0
18/04/06 12:56:30 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0 with generation 1
18/04/06 12:56:30 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-driver-0
18/04/06 12:56:31 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/04/06 12:56:32 INFO CodeGenerator: Code generated in 412.760978 ms
18/04/06 12:56:33 INFO CodeGenerator: Code generated in 67.180391 ms
18/04/06 12:56:35 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/04/06 12:56:35 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/04/06 12:56:35 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/04/06 12:56:35 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:56:35 INFO DAGScheduler: Missing parents: List()
18/04/06 12:56:35 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/04/06 12:56:35 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 22.5 KB, free 366.3 MB)
18/04/06 12:56:35 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.8 KB, free 366.3 MB)
18/04/06 12:56:35 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:41022 (size: 10.8 KB, free: 366.3 MB)
18/04/06 12:56:35 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/04/06 12:56:35 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/04/06 12:56:35 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/04/06 12:56:35 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:56:35 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/04/06 12:56:35 INFO Executor: Fetching file:/w205/full-stack/filtered_writes.py with timestamp 1523019380607
18/04/06 12:56:35 INFO Utils: /w205/full-stack/filtered_writes.py has been previously copied to /tmp/spark-d2418eaf-1129-4cb6-9290-7e5371b57111/userFiles-10d1a31a-72b2-46b9-8ba1-482583985513/filtered_writes.py
18/04/06 12:56:35 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:35 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:35 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:56:35 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:56:35 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor.
18/04/06 12:56:36 INFO CodeGenerator: Code generated in 63.654613 ms
18/04/06 12:56:36 INFO CodeGenerator: Code generated in 45.359326 ms
18/04/06 12:56:36 INFO CodeGenerator: Code generated in 32.237939 ms
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:37 INFO CodeGenerator: Code generated in 62.076294 ms
18/04/06 12:56:37 INFO PythonRunner: Times: total = 1072, boot = 828, init = 243, finish = 1
18/04/06 12:56:37 INFO PythonRunner: Times: total = 69, boot = 9, init = 60, finish = 0
18/04/06 12:56:37 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2092 bytes result sent to driver
18/04/06 12:56:37 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 2130 ms on localhost (executor driver) (1/1)
18/04/06 12:56:37 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/04/06 12:56:37 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 2.182 s
18/04/06 12:56:37 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 2.594298 s
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- timestamp: string (nullable = true)

18/04/06 12:56:37 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/04/06 12:56:38 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/06 12:56:38 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/04/06 12:56:38 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:56:38 INFO DAGScheduler: Missing parents: List()
18/04/06 12:56:38 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/06 12:56:38 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 27.5 KB, free 366.2 MB)
18/04/06 12:56:38 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 13.5 KB, free 366.2 MB)
18/04/06 12:56:38 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.6:41022 (size: 13.5 KB, free: 366.3 MB)
18/04/06 12:56:38 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/04/06 12:56:38 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[18] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/06 12:56:38 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/04/06 12:56:38 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:56:38 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/04/06 12:56:38 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:38 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:38 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:56:38 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:56:38 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor.
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:38 INFO PythonRunner: Times: total = 86, boot = -901, init = 986, finish = 1
18/04/06 12:56:38 INFO CodeGenerator: Code generated in 67.276073 ms
18/04/06 12:56:38 INFO PythonRunner: Times: total = 61, boot = 15, init = 43, finish = 3
18/04/06 12:56:38 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2214 bytes result sent to driver
18/04/06 12:56:38 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 473 ms on localhost (executor driver) (1/1)
18/04/06 12:56:38 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/04/06 12:56:38 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) finished in 0.485 s
18/04/06 12:56:38 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.585305 s
18/04/06 12:56:38 INFO CodeGenerator: Code generated in 67.810192 ms
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
+------+-----------------+---------------+--------------+--------------------+

18/04/06 12:56:40 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:41022 in memory (size: 10.8 KB, free: 366.3 MB)
18/04/06 12:56:40 INFO ContextCleaner: Cleaned accumulator 54
18/04/06 12:56:40 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.18.0.6:41022 in memory (size: 13.5 KB, free: 366.3 MB)
18/04/06 12:56:40 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/06 12:56:40 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/06 12:56:40 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/06 12:56:41 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
18/04/06 12:56:41 INFO DAGScheduler: Got job 2 (parquet at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/04/06 12:56:41 INFO DAGScheduler: Final stage: ResultStage 2 (parquet at NativeMethodAccessorImpl.java:0)
18/04/06 12:56:41 INFO DAGScheduler: Parents of final stage: List()
18/04/06 12:56:41 INFO DAGScheduler: Missing parents: List()
18/04/06 12:56:41 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[19] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
18/04/06 12:56:41 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 92.2 KB, free 366.2 MB)
18/04/06 12:56:41 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 36.3 KB, free 366.2 MB)
18/04/06 12:56:41 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.6:41022 (size: 36.3 KB, free: 366.3 MB)
18/04/06 12:56:41 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1006
18/04/06 12:56:41 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[19] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/04/06 12:56:41 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
18/04/06 12:56:41 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/04/06 12:56:41 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
18/04/06 12:56:41 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:41 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor
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

18/04/06 12:56:41 INFO AppInfoParser: Kafka version : 0.10.0.1
18/04/06 12:56:41 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/04/06 12:56:41 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-f37e0d32-bc7c-4833-b5e5-7fe59e89b865-executor.
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/04/06 12:56:41 INFO PythonRunner: Times: total = 116, boot = -3137, init = 3253, finish = 0
18/04/06 12:56:41 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/06 12:56:41 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/04/06 12:56:41 INFO CodecConfig: Compression: SNAPPY
18/04/06 12:56:41 INFO CodecConfig: Compression: SNAPPY
18/04/06 12:56:41 INFO ParquetOutputFormat: Parquet block size to 134217728
18/04/06 12:56:41 INFO ParquetOutputFormat: Parquet page size to 1048576
18/04/06 12:56:41 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
18/04/06 12:56:41 INFO ParquetOutputFormat: Dictionary is on
18/04/06 12:56:41 INFO ParquetOutputFormat: Validation is off
18/04/06 12:56:41 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
18/04/06 12:56:41 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
18/04/06 12:56:41 INFO ParquetOutputFormat: Page size checking is: estimated
18/04/06 12:56:41 INFO ParquetOutputFormat: Min row count for page size check is: 100
18/04/06 12:56:41 INFO ParquetOutputFormat: Max row count for page size check is: 10000
18/04/06 12:56:41 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
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

       
18/04/06 12:56:42 INFO CodecPool: Got brand-new compressor [.snappy]
18/04/06 12:56:42 INFO PythonRunner: Times: total = 56, boot = -3169, init = 3221, finish = 4
18/04/06 12:56:42 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 1018
18/04/06 12:56:44 INFO FileOutputCommitter: Saved output of task 'attempt_20180406125641_0002_m_000000_0' to hdfs://cloudera/tmp/purchases/_temporary/0/task_20180406125641_0002_m_000000
18/04/06 12:56:44 INFO SparkHadoopMapRedUtil: attempt_20180406125641_0002_m_000000_0: Committed
18/04/06 12:56:44 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2325 bytes result sent to driver
18/04/06 12:56:44 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 3249 ms on localhost (executor driver) (1/1)
18/04/06 12:56:44 INFO DAGScheduler: ResultStage 2 (parquet at NativeMethodAccessorImpl.java:0) finished in 3.269 s
18/04/06 12:56:44 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/04/06 12:56:44 INFO DAGScheduler: Job 2 finished: parquet at NativeMethodAccessorImpl.java:0, took 3.333915 s
18/04/06 12:56:44 INFO FileFormatWriter: Job null committed.
18/04/06 12:56:45 INFO SparkContext: Invoking stop() from shutdown hook
18/04/06 12:56:45 INFO SparkUI: Stopped Spark web UI at http://172.18.0.6:4040
18/04/06 12:56:45 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/04/06 12:56:45 INFO MemoryStore: MemoryStore cleared
18/04/06 12:56:45 INFO BlockManager: BlockManager stopped
18/04/06 12:56:45 INFO BlockManagerMaster: BlockManagerMaster stopped
18/04/06 12:56:45 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/04/06 12:56:45 INFO SparkContext: Successfully stopped SparkContext
18/04/06 12:56:45 INFO ShutdownHookManager: Shutdown hook called
18/04/06 12:56:45 INFO ShutdownHookManager: Deleting directory /tmp/spark-d2418eaf-1129-4cb6-9290-7e5371b57111/pyspark-6e6c2e21-77f6-4e56-a639-9f389c5e267b
18/04/06 12:56:45 INFO ShutdownHookManager: Deleting directory /tmp/spark-d2418eaf-1129-4cb6-9290-7e5371b57111
```

##### Verify that the files have been written to HDFS #####

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/
```

```
Found 3 items
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-04-06 05:08 /tmp/hive
drwxr-xr-x   - root   supergroup          0 2018-04-06 12:56 /tmp/purchases
```

```bash
$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases
```

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-04-06 12:56 /tmp/purchases/_SUCCESS
-rw-r--r--   1 root supergroup       1638 2018-04-06 12:56 /tmp/purchases/part-00000-611ec824-a5eb-4480-a49a-4bb9312a0e35-c000.snappy.parquet
```

The ls commands returns the HDFS file structure showing that the parquet file has been successfully created

##### Start up Jupyter Notebook #####

```bash
$ docker-compose exec spark env PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 8888 --ip 0.0.0.0 --allow-root' pyspark
```

```
[I 13:10:28.128 NotebookApp] Writing notebook server cookie secret to /root/.local/share/jupyter/runtime/notebook_cookie_secret
[I 13:10:28.265 NotebookApp] Serving notebooks from local directory: /spark-2.2.0-bin-hadoop2.6
[I 13:10:28.265 NotebookApp] 0 active kernels 
[I 13:10:28.265 NotebookApp] The Jupyter Notebook is running at: http://0.0.0.0:8888/?token=186a756620efcb83fca55f8734e9ca305c7f524fd1a9d61a
[I 13:10:28.265 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 13:10:28.266 NotebookApp] 
    
    Copy/paste this URL into your browser when you connect for the first time,
    to login with a token:
        http://0.0.0.0:8888/?token=186a756620efcb83fca55f8734e9ca305c7f524fd1a9d61a
```

Open the URL http://0.0.0.0:8888/?token=186a756620efcb83fca55f8734e9ca305c7f524fd1a9d61a in browser replacing 0.0.0.0 with your Droplet IP address

##### Create a new Jupyter Notebook #####

##### Get the path to the current directory in the notebook ######

```python
import os
os.getcwd()
```

```
/spark-2.2.0-bin-hadoop2.6
```

##### Run bash in the Spark package context #####

```bash
docker-compose exec spark bash
```

```bash
root@5778e842f83e:/spark-2.2.0-bin-hadoop2.6# 
```

##### Create a symlink to the volume /w205  #####

```bash
ln -s /w205 .
```

A symlink to the volume /w205 has been created.

```bash
ls -lFa
total 128
drwxr-xr-x 1  500  500  4096 Apr  6 13:29 ./
drwxr-xr-x 1 root root  4096 Apr  6 05:07 ../
drwxr-xr-x 2 root root  4096 Apr  6 13:14 .ipynb_checkpoints/
-rw-r--r-- 1  500  500 17881 Jun 30  2017 LICENSE
-rw-r--r-- 1  500  500 24645 Jun 30  2017 NOTICE
drwxr-xr-x 3  500  500  4096 Jun 30  2017 R/
-rw-r--r-- 1  500  500  3809 Jun 30  2017 README.md
-rw-r--r-- 1  500  500   128 Jun 30  2017 RELEASE
-rw-r--r-- 1 root root   518 Apr  6 13:16 Untitled.ipynb
drwxr-xr-x 2  500  500  4096 Jun 30  2017 bin/
drwxr-xr-x 1  500  500  4096 Apr  6 05:07 conf/
drwxr-xr-x 5  500  500  4096 Jun 30  2017 data/
-rw-r--r-- 1 root root   698 Apr  6 13:14 derby.log
lrwxrwxrwx 1 root root    34 Feb 18 22:18 entrypoint.sh -> usr/local/bin/docker-entrypoint.sh
drwxr-xr-x 4  500  500  4096 Jun 30  2017 examples/
drwxr-xr-x 1  500  500  4096 Feb 18 22:18 jars/
drwxr-xr-x 2  500  500  4096 Jun 30  2017 licenses/
drwxr-xr-x 5 root root  4096 Apr  6 13:14 metastore_db/
drwxr-xr-x 1  500  500  4096 Jun 30  2017 python/
drwxr-xr-x 2  500  500  4096 Jun 30  2017 sbin/
drwxr-xr-x 2 root root  4096 Apr  6 12:02 spark-warehouse/
drwxr-xr-x 2 root root  4096 Feb 18 22:18 templates/
lrwxrwxrwx 1 root root     5 Apr  6 13:29 w205 -> /w205/
drwxr-xr-x 2  500  500  4096 Jun 30  2017 yarn
```

Confirm that the directory /w205 shows up on Jupyter Notebook.

##### Create a new Jupyter Notebook in the directory activity-11-takoloco under the volume /w205 #####

###### Read data from parquet and show ######

```python
purchases = spark.read.parquet('/tmp/purchases')
purchases.show()
```

```
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
|   */*|    user2.att.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:29:...|
+------+-----------------+---------------+--------------+--------------------+
```


###### Create a temporary table purchases with the data loaded ######

```python
purchases.registerTempTable('purchases')
```

###### Use SQL to select purchase data with the host 'user1.comcast.com' and show ######

```python
purchases_by_example2 = spark.sql("select * from purchases where Host = 'user1.comcast.com'")
purchases_by_example2.show()
```

```
+------+-----------------+---------------+--------------+--------------------+
|Accept|             Host|     User-Agent|    event_type|           timestamp|
+------+-----------------+---------------+--------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
|   */*|user1.comcast.com|ApacheBench/2.3|purchase_sword|2018-04-06 05:28:...|
+------+-----------------+---------------+--------------+--------------------+
```

###### Load data into a Pandas dataframe and show ######

```python
df = purchases_by_example2.toPandas()
df.describe()
```

```
	Accept	Host	User-Agent	event_type	timestamp
count	10	10	10	10	10
unique	1	1	1	1	10
top	*/*	user1.comcast.com	ApacheBench/2.3	purchase_sword	2018-04-06 05:28:13.104
freq	10	10	10	10	1
```

##### Stop the web server, kafkacat and Jupyter Notebook with a control-C #####

###### Web server ######

```
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
^Cscience@w205s7-crook-6:~/w205/activity-11-takoloco$
```

The web server process is stopped.

###### Kafkacat ######

```
{"Accept-Encoding": "gzip", "Host": "159.65.78.35:5000", "Accept": "*/*", "User-Agent": "Mozilla/5.0 zgrab/0.x", "Connection": "close", "event_type": "default"}
^Cscience@w205s7-crook-6:~/w205/activity-11-takoloco$
```

###### Jupyter Notebook ######

```
[I 13:57:09.961 NotebookApp] Saving file at /w205/activity-11-takoloco/spark_read_parquet_from_hdfs_register_as_sql.ipynb
^C[I 14:02:18.283 NotebookApp] interrupted
Serving notebooks from local directory: /spark-2.2.0-bin-hadoop2.6
1 active kernels 
The Jupyter Notebook is running at: http://0.0.0.0:8888/?token=191fb27c2a467ecbf409afa87aabad169701b00271dd4761
Shutdown this notebook server (y/[n])? y
[C 14:02:20.085 NotebookApp] Shutdown confirmed
[I 14:02:20.086 NotebookApp] Shutting down kernels
[I 14:02:21.608 NotebookApp] Kernel shutdown: 284cb940-0d52-496a-9152-525ceb4cca49
science@w205s7-crook-6:~/w205/activity-11-takoloco$
```

##### Take down the cluster #####

```bash
docker-compose down
```

```
Stopping activity11takoloco_spark_1 ... done
Stopping activity11takoloco_kafka_1 ... done
Stopping activity11takoloco_mids_1 ... done
Stopping activity11takoloco_cloudera_1 ... done
Stopping activity11takoloco_zookeeper_1 ... done
Removing activity11takoloco_spark_1 ... done
Removing activity11takoloco_kafka_1 ... done
Removing activity11takoloco_mids_1 ... done
Removing activity11takoloco_cloudera_1 ... done
Removing activity11takoloco_zookeeper_1 ... done
Removing network activity11takoloco_default
```

The docker cluster and all of its services defined in docker-compose.yaml are stopped.

##### Confirm all containers have been properly shut down #####

```bash
docker-compose ps
```

```
Name   Command   State   Ports 
------------------------------
```

```bash
docker ps -a
```

```
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
```

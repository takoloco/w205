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

# Assignment 10

## Follow the steps we did in class 

## Follow the steps we did in class 

### Turn in your `/assignment-10-<user-name>/README.md` file. It should include:

#### 1) A summary type explanation of the example.
  * Start up the docker cluster
  * Create a topic on Kafka
  * Create a python web server script using the flask library
  * Start up the web server
  * Access the web server via the curl command
  * Use kafkacat to consume the messages and confirm they match what you have published in the previous step
  * Create a python script for extracting events using Spark
  * Execute extract_events.py to extract events from kafka via Spark and write to HDFS
  * Verify that the files have been written to HDFS
  * Create a python script for extracting events from kafka and transforming them using Spark
  * Execute transform_events.py to extract events from kafka and transforming them via Spark and write to HDFS
  * Verify that the files have been written to HDFS
  * Create a python script for extracting events from kafka and separating them by event type using Spark
  * Execute separate_events.py to extract events from kafka and separate them by event type via Spark
  * Stop the web server with a control-C
  * Take down the cluster

#### 2) Your `docker-compose.yml` ####

#### 3) Source code for the flask application(s) used. ####

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
Creating network "activity10takoloco_default" with the default driver
Creating activity10takoloco_cloudera_1
Creating activity10takoloco_mids_1
Creating activity10takoloco_zookeeper_1
Creating activity10takoloco_kafka_1
Creating activity10takoloco_spark_1
```

Start up the Docker cluster defined in the docker-compose.yaml which include the following services:

* cloudera
* mids
* zookeeper
* kafka
* spark

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
vim game_api.py
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
$ docker-compose exec mids env FLASK_APP=/w205/activity-10-takoloco/game_api.py flask run --host 0.0.0.0
```

```
 * Serving Flask app "game_api"
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Start up a Flask web server process with the Python file /w205/activity-10-takoloco/game_api.py

##### Access the web server via the curl command #####

```bash
$ docker-compose exec mids curl http://localhost:5000/
```

###### Client ######

```
This is the default response!
```

###### Server ######

```
127.0.0.1 - - [23/Mar/2018 11:02:23] "GET / HTTP/1.1" 200 -
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/". The web server responds with the text sring "This is the default response!".

```bash
$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword
```

###### Client ######

```
Sword Purchased!
```

###### Server ######

```
127.0.0.1 - - [23/Mar/2018 11:04:52] "GET /purchase_a_sword HTTP/1.1" 200 -
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/purchase_a_sword". The web server responds with the text sring "Sword Purchased!".

##### Use kafkacat to consume the messages and confirm they match what you have published in the previous step #####

```bash
$ docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning -e
```

```
{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
{"Host": "localhost:5000", "event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
% Reached end of topic events [0] at offset 10: exiting
```

Consumes all the messages published to the Kafka topic "events"

##### Create a python script for extracting events using Spark #####

```bash
vim extract_events.py
```

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""

import json
from pyspark.sql import SparkSession


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

    events = raw_events.select(raw_events.value.cast('string'))
    extracted_events = events.rdd.map(lambda x: json.loads(x.value)).toDF()

    extracted_events \
        .write \
        .parquet("/tmp/extracted_events")


if __name__ == "__main__":
    main()
```

Create a file called extract_events.py and save the file to the file system after editing

##### Execute extract_events.py to extract events from kafka via Spark and write to HDFS #####

```bash
$ docker-compose exec spark spark-submit /w205/spark-from-files/extract_events.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/03/23 11:14:45 INFO SparkContext: Running Spark version 2.2.0
18/03/23 11:14:45 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/03/23 11:14:45 INFO SparkContext: Submitted application: ExtractEventsJob
18/03/23 11:14:45 INFO SecurityManager: Changing view acls to: root
18/03/23 11:14:45 INFO SecurityManager: Changing modify acls to: root
18/03/23 11:14:45 INFO SecurityManager: Changing view acls groups to: 
18/03/23 11:14:45 INFO SecurityManager: Changing modify acls groups to: 
18/03/23 11:14:45 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/03/23 11:14:46 INFO Utils: Successfully started service 'sparkDriver' on port 42165.
18/03/23 11:14:46 INFO SparkEnv: Registering MapOutputTracker
18/03/23 11:14:46 INFO SparkEnv: Registering BlockManagerMaster
18/03/23 11:14:46 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/23 11:14:46 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/23 11:14:46 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-19383742-8b78-46b8-9422-ff83ecb4a00d
18/03/23 11:14:46 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/03/23 11:14:46 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/23 11:14:47 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/23 11:14:47 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.6:4040
18/03/23 11:14:47 INFO SparkContext: Added file file:/w205/spark-from-files/extract_events.py at file:/w205/spark-from-files/extract_events.py with timestamp 1521803687638
18/03/23 11:14:47 INFO Utils: Copying /w205/spark-from-files/extract_events.py to /tmp/spark-27b1e629-e33c-4929-9f24-ddf44c4f4398/userFiles-3211502a-481f-49e1-b392-c6ca09247b9a/extract_events.py
18/03/23 11:14:47 INFO Executor: Starting executor ID driver on host localhost
18/03/23 11:14:47 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 40873.
18/03/23 11:14:47 INFO NettyBlockTransferService: Server created on 172.18.0.6:40873
18/03/23 11:14:47 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/23 11:14:47 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.6, 40873, None)
18/03/23 11:14:47 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:40873 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.6, 40873, None)
18/03/23 11:14:47 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.6, 40873, None)
18/03/23 11:14:47 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.6, 40873, None)
18/03/23 11:14:48 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse').
18/03/23 11:14:48 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse'.
18/03/23 11:14:49 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/03/23 11:14:52 INFO CatalystSqlParser: Parsing command: string
18/03/23 11:14:52 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0
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

18/03/23 11:14:52 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0
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

18/03/23 11:14:52 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:14:52 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:14:54 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0.
18/03/23 11:14:54 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0
18/03/23 11:14:54 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0
18/03/23 11:14:57 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0 with generation 1
18/03/23 11:14:57 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-driver-0
18/03/23 11:14:57 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/03/23 11:14:58 INFO CodeGenerator: Code generated in 371.602167 ms
18/03/23 11:15:00 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/03/23 11:15:00 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/03/23 11:15:00 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/03/23 11:15:00 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:15:00 INFO DAGScheduler: Missing parents: List()
18/03/23 11:15:00 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[9] at RDD at PythonRDD.scala:48), which has no missing parents
18/03/23 11:15:01 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 15.0 KB, free 366.3 MB)
18/03/23 11:15:01 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 8.1 KB, free 366.3 MB)
18/03/23 11:15:01 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:40873 (size: 8.1 KB, free: 366.3 MB)
18/03/23 11:15:01 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/03/23 11:15:01 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[9] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/03/23 11:15:01 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/03/23 11:15:01 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:15:01 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/03/23 11:15:01 INFO Executor: Fetching file:/w205/spark-from-files/extract_events.py with timestamp 1521803687638
18/03/23 11:15:01 INFO Utils: /w205/spark-from-files/extract_events.py has been previously copied to /tmp/spark-27b1e629-e33c-4929-9f24-ddf44c4f4398/userFiles-3211502a-481f-49e1-b392-c6ca09247b9a/extract_events.py
18/03/23 11:15:01 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor
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

18/03/23 11:15:01 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor
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

18/03/23 11:15:01 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:15:01 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:15:01 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor.
18/03/23 11:15:01 INFO CodeGenerator: Code generated in 76.589941 ms
18/03/23 11:15:01 INFO CodeGenerator: Code generated in 65.820851 ms
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:02 INFO PythonRunner: Times: total = 886, boot = 797, init = 88, finish = 1
18/03/23 11:15:02 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1793 bytes result sent to driver
18/03/23 11:15:02 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1516 ms on localhost (executor driver) (1/1)
18/03/23 11:15:02 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/23 11:15:02 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 1.573 s
18/03/23 11:15:02 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 2.028360 s
/spark-2.2.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/sql/session.py:351: UserWarning: Using RDD of dict to inferSchema is deprecated. Use pyspark.sql.Row instead
  warnings.warn("Using RDD of dict to inferSchema is deprecated. "
18/03/23 11:15:03 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:40873 in memory (size: 8.1 KB, free: 366.3 MB)
18/03/23 11:15:04 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:15:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:15:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:15:04 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
18/03/23 11:15:04 INFO DAGScheduler: Got job 1 (parquet at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:15:04 INFO DAGScheduler: Final stage: ResultStage 1 (parquet at NativeMethodAccessorImpl.java:0)
18/03/23 11:15:04 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:15:04 INFO DAGScheduler: Missing parents: List()
18/03/23 11:15:04 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[14] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:15:04 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 84.4 KB, free 366.2 MB)
18/03/23 11:15:04 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 34.2 KB, free 366.2 MB)
18/03/23 11:15:04 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.6:40873 (size: 34.2 KB, free: 366.3 MB)
18/03/23 11:15:04 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/03/23 11:15:04 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[14] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:15:04 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/03/23 11:15:04 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:15:04 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/03/23 11:15:04 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor
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

18/03/23 11:15:04 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor
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

18/03/23 11:15:04 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:15:04 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:15:04 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-5fc8dedc-c89e-42d8-914c-c4f62e50b1e8-executor.
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:15:04 INFO CodeGenerator: Code generated in 23.043367 ms
18/03/23 11:15:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:15:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:15:04 INFO CodecConfig: Compression: SNAPPY
18/03/23 11:15:04 INFO CodecConfig: Compression: SNAPPY
18/03/23 11:15:04 INFO ParquetOutputFormat: Parquet block size to 134217728
18/03/23 11:15:04 INFO ParquetOutputFormat: Parquet page size to 1048576
18/03/23 11:15:04 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
18/03/23 11:15:04 INFO ParquetOutputFormat: Dictionary is on
18/03/23 11:15:04 INFO ParquetOutputFormat: Validation is off
18/03/23 11:15:04 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
18/03/23 11:15:04 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
18/03/23 11:15:04 INFO ParquetOutputFormat: Page size checking is: estimated
18/03/23 11:15:04 INFO ParquetOutputFormat: Min row count for page size check is: 100
18/03/23 11:15:04 INFO ParquetOutputFormat: Max row count for page size check is: 10000
18/03/23 11:15:04 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
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
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary Accept (UTF8);
  optional binary Host (UTF8);
  optional binary User-Agent (UTF8);
  optional binary event_type (UTF8);
}

       
18/03/23 11:15:04 INFO CodecPool: Got brand-new compressor [.snappy]
18/03/23 11:15:05 INFO PythonRunner: Times: total = 60, boot = 6, init = 53, finish = 1
18/03/23 11:15:05 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 229
18/03/23 11:15:06 INFO FileOutputCommitter: Saved output of task 'attempt_20180323111504_0001_m_000000_0' to hdfs://cloudera/tmp/extracted_events/_temporary/0/task_20180323111504_0001_m_000000
18/03/23 11:15:06 INFO SparkHadoopMapRedUtil: attempt_20180323111504_0001_m_000000_0: Committed
18/03/23 11:15:06 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2173 bytes result sent to driver
18/03/23 11:15:06 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 2263 ms on localhost (executor driver) (1/1)
18/03/23 11:15:06 INFO DAGScheduler: ResultStage 1 (parquet at NativeMethodAccessorImpl.java:0) finished in 2.268 s
18/03/23 11:15:06 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/23 11:15:06 INFO DAGScheduler: Job 1 finished: parquet at NativeMethodAccessorImpl.java:0, took 2.320559 s
18/03/23 11:15:06 INFO FileFormatWriter: Job null committed.
18/03/23 11:15:06 INFO SparkContext: Invoking stop() from shutdown hook
18/03/23 11:15:06 INFO SparkUI: Stopped Spark web UI at http://172.18.0.6:4040
18/03/23 11:15:06 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/23 11:15:06 INFO MemoryStore: MemoryStore cleared
18/03/23 11:15:06 INFO BlockManager: BlockManager stopped
18/03/23 11:15:06 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/23 11:15:07 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/23 11:15:07 INFO SparkContext: Successfully stopped SparkContext
18/03/23 11:15:07 INFO ShutdownHookManager: Shutdown hook called
18/03/23 11:15:07 INFO ShutdownHookManager: Deleting directory /tmp/spark-27b1e629-e33c-4929-9f24-ddf44c4f4398/pyspark-bc0e24fb-1e4e-441a-b7b0-47058550d2c5
18/03/23 11:15:07 INFO ShutdownHookManager: Deleting directory /tmp/spark-27b1e629-e33c-4929-9f24-ddf44c4f4398
```

extract_events.py extracts events from kafka and writes them out to HDFS via Spark.

##### Verify that the files have been written to HDFS #####

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/
```

```
Found 3 items
drwxr-xr-x   - root   supergroup          0 2018-03-23 11:15 /tmp/extracted_events
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-03-23 10:38 /tmp/hive
```

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/extracted_events/
```

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-03-23 11:15 /tmp/extracted_events/_SUCCESS
-rw-r--r--   1 root supergroup       1189 2018-03-23 11:15 /tmp/extracted_events/part-00000-1d42d646-dc94-4a96-b311-06d3af797631-c000.snappy.parquet
```

The ls commands returns the HDFS file structure showing that the parquet file has been successfully created.

##### Create a python script for extracting events from kafka and transforming them using Spark #####

```bash
vim transform_events.py 
```

```python
#!/usr/bin/env python
"""Extract events from kafka, transform, and write to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Host'] = "moe" # silly change to show it works
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
    munged_events.show()

    extracted_events = munged_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_events.show()

    extracted_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/extracted_events")


if __name__ == "__main__":
    main()
```

Create a file called transform_events.py and save the file to the file system after editing

##### Execute transform_events.py to extract events from kafka and transforming them via Spark and write to HDFS #####

```bash
$ docker-compose exec spark spark-submit /w205/spark-from-files/transform_events.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/03/23 11:49:11 INFO SparkContext: Running Spark version 2.2.0
18/03/23 11:49:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/03/23 11:49:12 INFO SparkContext: Submitted application: ExtractEventsJob
18/03/23 11:49:12 INFO SecurityManager: Changing view acls to: root
18/03/23 11:49:12 INFO SecurityManager: Changing modify acls to: root
18/03/23 11:49:12 INFO SecurityManager: Changing view acls groups to: 
18/03/23 11:49:12 INFO SecurityManager: Changing modify acls groups to: 
18/03/23 11:49:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/03/23 11:49:12 INFO Utils: Successfully started service 'sparkDriver' on port 37463.
18/03/23 11:49:12 INFO SparkEnv: Registering MapOutputTracker
18/03/23 11:49:12 INFO SparkEnv: Registering BlockManagerMaster
18/03/23 11:49:12 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/23 11:49:12 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/23 11:49:12 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-934b1ffc-673d-404c-b830-b3a3d5e6296d
18/03/23 11:49:12 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/03/23 11:49:13 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/23 11:49:13 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/23 11:49:13 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.5:4040
18/03/23 11:49:13 INFO SparkContext: Added file file:/w205/spark-from-files/transform_events.py at file:/w205/spark-from-files/transform_events.py with timestamp 1521805753577
18/03/23 11:49:13 INFO Utils: Copying /w205/spark-from-files/transform_events.py to /tmp/spark-b855e396-94f3-43a6-8312-8c8ac4ad48ad/userFiles-3bb5473a-a084-41db-8fbd-f1c9331d17c7/transform_events.py
18/03/23 11:49:13 INFO Executor: Starting executor ID driver on host localhost
18/03/23 11:49:13 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 37823.
18/03/23 11:49:13 INFO NettyBlockTransferService: Server created on 172.18.0.5:37823
18/03/23 11:49:13 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/23 11:49:13 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.5, 37823, None)
18/03/23 11:49:13 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:37823 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.5, 37823, None)
18/03/23 11:49:13 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.5, 37823, None)
18/03/23 11:49:13 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.5, 37823, None)
18/03/23 11:49:14 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/03/23 11:49:14 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/03/23 11:49:15 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/03/23 11:49:17 INFO CatalystSqlParser: Parsing command: string
18/03/23 11:49:17 INFO CatalystSqlParser: Parsing command: string
18/03/23 11:49:18 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0
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

18/03/23 11:49:18 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0
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

18/03/23 11:49:18 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:18 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:18 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0.
18/03/23 11:49:18 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0
18/03/23 11:49:18 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0
18/03/23 11:49:21 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0 with generation 1
18/03/23 11:49:21 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-driver-0
18/03/23 11:49:21 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/03/23 11:49:22 INFO CodeGenerator: Code generated in 327.046457 ms
18/03/23 11:49:23 INFO CodeGenerator: Code generated in 35.233063 ms
18/03/23 11:49:23 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/03/23 11:49:23 INFO DAGScheduler: Got job 0 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:49:23 INFO DAGScheduler: Final stage: ResultStage 0 (showString at NativeMethodAccessorImpl.java:0)
18/03/23 11:49:23 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:49:23 INFO DAGScheduler: Missing parents: List()
18/03/23 11:49:23 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[10] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:49:23 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 18.9 KB, free 366.3 MB)
18/03/23 11:49:23 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 8.9 KB, free 366.3 MB)
18/03/23 11:49:23 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.5:37823 (size: 8.9 KB, free: 366.3 MB)
18/03/23 11:49:23 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/03/23 11:49:23 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[10] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:49:23 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/03/23 11:49:23 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:49:23 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/03/23 11:49:23 INFO Executor: Fetching file:/w205/spark-from-files/transform_events.py with timestamp 1521805753577
18/03/23 11:49:23 INFO Utils: /w205/spark-from-files/transform_events.py has been previously copied to /tmp/spark-b855e396-94f3-43a6-8312-8c8ac4ad48ad/userFiles-3bb5473a-a084-41db-8fbd-f1c9331d17c7/transform_events.py
18/03/23 11:49:23 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-executor
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

18/03/23 11:49:23 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-executor
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

18/03/23 11:49:23 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:23 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:23 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-3acac0c0-d534-4765-ba56-2524e9ed4fda-executor.
18/03/23 11:49:24 INFO CodeGenerator: Code generated in 46.404446 ms
18/03/23 11:49:24 INFO CodeGenerator: Code generated in 75.2557 ms
18/03/23 11:49:24 INFO CodeGenerator: Code generated in 26.190024 ms
18/03/23 11:49:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:25 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:25 INFO CodeGenerator: Code generated in 44.68884 ms
18/03/23 11:49:25 INFO PythonRunner: Times: total = 842, boot = 754, init = 87, finish = 1
18/03/23 11:49:25 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2066 bytes result sent to driver
18/03/23 11:49:25 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1572 ms on localhost (executor driver) (1/1)
18/03/23 11:49:25 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/23 11:49:25 INFO DAGScheduler: ResultStage 0 (showString at NativeMethodAccessorImpl.java:0) finished in 1.607 s
18/03/23 11:49:25 INFO DAGScheduler: Job 0 finished: showString at NativeMethodAccessorImpl.java:0, took 1.921799 s
18/03/23 11:49:25 INFO CodeGenerator: Code generated in 33.730885 ms
+--------------------+--------------------+--------------------+
|                 raw|           timestamp|              munged|
+--------------------+--------------------+--------------------+
|{"Host": "localho...|2018-03-23 11:47:...|{"Host": "moe", "...|
|{"Host": "localho...|2018-03-23 11:47:...|{"Host": "moe", "...|
|{"Host": "localho...|2018-03-23 11:47:...|{"Host": "moe", "...|
|{"Host": "localho...|2018-03-23 11:47:...|{"Host": "moe", "...|
|{"Host": "localho...|2018-03-23 11:47:...|{"Host": "moe", "...|
+--------------------+--------------------+--------------------+

18/03/23 11:49:25 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0
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

18/03/23 11:49:25 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0
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

18/03/23 11:49:25 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:25 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:25 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0.
18/03/23 11:49:25 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0
18/03/23 11:49:25 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0
18/03/23 11:49:28 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0 with generation 1
18/03/23 11:49:28 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-driver-0
18/03/23 11:49:28 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/03/23 11:49:29 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/03/23 11:49:29 INFO DAGScheduler: Got job 1 (runJob at PythonRDD.scala:446) with 1 output partitions
18/03/23 11:49:29 INFO DAGScheduler: Final stage: ResultStage 1 (runJob at PythonRDD.scala:446)
18/03/23 11:49:29 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:49:29 INFO DAGScheduler: Missing parents: List()
18/03/23 11:49:29 INFO DAGScheduler: Submitting ResultStage 1 (PythonRDD[23] at RDD at PythonRDD.scala:48), which has no missing parents
18/03/23 11:49:29 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 21.4 KB, free 366.3 MB)
18/03/23 11:49:29 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 10.5 KB, free 366.2 MB)
18/03/23 11:49:29 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.5:37823 (size: 10.5 KB, free: 366.3 MB)
18/03/23 11:49:29 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/03/23 11:49:29 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (PythonRDD[23] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/03/23 11:49:29 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/03/23 11:49:29 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:49:29 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/03/23 11:49:29 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:29 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:29 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:29 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:29 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor.
18/03/23 11:49:29 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:29 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:29 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:29 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:29 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:29 INFO PythonRunner: Times: total = 42, boot = -4709, init = 4750, finish = 1
18/03/23 11:49:29 INFO PythonRunner: Times: total = 35, boot = 10, init = 24, finish = 1
18/03/23 11:49:29 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1999 bytes result sent to driver
18/03/23 11:49:29 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 244 ms on localhost (executor driver) (1/1)
18/03/23 11:49:29 INFO DAGScheduler: ResultStage 1 (runJob at PythonRDD.scala:446) finished in 0.237 s
18/03/23 11:49:29 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/23 11:49:29 INFO DAGScheduler: Job 1 finished: runJob at PythonRDD.scala:446, took 0.294022 s
18/03/23 11:49:30 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/03/23 11:49:30 INFO DAGScheduler: Got job 2 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:49:30 INFO DAGScheduler: Final stage: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0)
18/03/23 11:49:30 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:49:30 INFO DAGScheduler: Missing parents: List()
18/03/23 11:49:30 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[29] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:49:30 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 26.6 KB, free 366.2 MB)
18/03/23 11:49:30 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 13.3 KB, free 366.2 MB)
18/03/23 11:49:30 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.5:37823 (size: 13.3 KB, free: 366.3 MB)
18/03/23 11:49:30 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1006
18/03/23 11:49:30 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[29] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:49:30 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
18/03/23 11:49:30 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:49:30 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
18/03/23 11:49:30 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:30 INFO ConsumerConfig: ConsumerConfig values: 
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
	client.id = consumer-5
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:30 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:30 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:30 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor.
18/03/23 11:49:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:30 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:30 INFO PythonRunner: Times: total = 40, boot = -334, init = 373, finish = 1
18/03/23 11:49:30 INFO CodeGenerator: Code generated in 36.943432 ms
18/03/23 11:49:30 INFO PythonRunner: Times: total = 23, boot = 5, init = 17, finish = 1
18/03/23 11:49:30 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2039 bytes result sent to driver
18/03/23 11:49:30 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 280 ms on localhost (executor driver) (1/1)
18/03/23 11:49:30 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/03/23 11:49:30 INFO DAGScheduler: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0) finished in 0.281 s
18/03/23 11:49:30 INFO DAGScheduler: Job 2 finished: showString at NativeMethodAccessorImpl.java:0, took 0.314095 s
18/03/23 11:49:30 INFO CodeGenerator: Code generated in 35.031053 ms
+------+-------------+----+-----------+--------------+--------------------+
|Accept|Cache-Control|Host| User-Agent|    event_type|           timestamp|
+------+-------------+----+-----------+--------------+--------------------+
|   */*|     no-cache| moe|curl/7.47.0|       default|2018-03-23 11:47:...|
|   */*|     no-cache| moe|curl/7.47.0|       default|2018-03-23 11:47:...|
|   */*|     no-cache| moe|curl/7.47.0|       default|2018-03-23 11:47:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:47:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:47:...|
+------+-------------+----+-----------+--------------+--------------------+

18/03/23 11:49:30 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 172.18.0.5:37823 in memory (size: 13.3 KB, free: 366.3 MB)
18/03/23 11:49:30 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.5:37823 in memory (size: 8.9 KB, free: 366.3 MB)
18/03/23 11:49:30 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.18.0.5:37823 in memory (size: 10.5 KB, free: 366.3 MB)
18/03/23 11:49:30 INFO ContextCleaner: Cleaned accumulator 81
18/03/23 11:49:31 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:49:31 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:49:31 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:49:31 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
18/03/23 11:49:31 INFO DAGScheduler: Got job 3 (parquet at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:49:31 INFO DAGScheduler: Final stage: ResultStage 3 (parquet at NativeMethodAccessorImpl.java:0)
18/03/23 11:49:31 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:49:31 INFO DAGScheduler: Missing parents: List()
18/03/23 11:49:31 INFO DAGScheduler: Submitting ResultStage 3 (MapPartitionsRDD[30] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:49:31 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 91.4 KB, free 366.2 MB)
18/03/23 11:49:31 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 36.2 KB, free 366.2 MB)
18/03/23 11:49:31 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.18.0.5:37823 (size: 36.2 KB, free: 366.3 MB)
18/03/23 11:49:31 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1006
18/03/23 11:49:31 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 3 (MapPartitionsRDD[30] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:49:31 INFO TaskSchedulerImpl: Adding task set 3.0 with 1 tasks
18/03/23 11:49:31 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 3, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:49:31 INFO Executor: Running task 0.0 in stage 3.0 (TID 3)
18/03/23 11:49:31 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:31 INFO ConsumerConfig: ConsumerConfig values: 
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
	client.id = consumer-6
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
	group.id = spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor
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

18/03/23 11:49:31 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:49:31 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:49:31 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-06a5521b-b6a8-4de5-b692-f53482ef2072-executor.
18/03/23 11:49:31 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:31 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:31 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:31 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:31 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:49:31 INFO PythonRunner: Times: total = 38, boot = -1334, init = 1371, finish = 1
18/03/23 11:49:31 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:49:31 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
18/03/23 11:49:31 INFO CodecConfig: Compression: SNAPPY
18/03/23 11:49:31 INFO CodecConfig: Compression: SNAPPY
18/03/23 11:49:31 INFO ParquetOutputFormat: Parquet block size to 134217728
18/03/23 11:49:31 INFO ParquetOutputFormat: Parquet page size to 1048576
18/03/23 11:49:31 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
18/03/23 11:49:31 INFO ParquetOutputFormat: Dictionary is on
18/03/23 11:49:31 INFO ParquetOutputFormat: Validation is off
18/03/23 11:49:31 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
18/03/23 11:49:31 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
18/03/23 11:49:31 INFO ParquetOutputFormat: Page size checking is: estimated
18/03/23 11:49:31 INFO ParquetOutputFormat: Min row count for page size check is: 100
18/03/23 11:49:31 INFO ParquetOutputFormat: Max row count for page size check is: 10000
18/03/23 11:49:31 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "Accept",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Cache-Control",
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
  optional binary Cache-Control (UTF8);
  optional binary Host (UTF8);
  optional binary User-Agent (UTF8);
  optional binary event_type (UTF8);
  optional binary timestamp (UTF8);
}

       
18/03/23 11:49:31 INFO CodecPool: Got brand-new compressor [.snappy]
18/03/23 11:49:32 INFO PythonRunner: Times: total = 42, boot = -1324, init = 1366, finish = 0
18/03/23 11:49:32 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 325
18/03/23 11:49:32 INFO FileOutputCommitter: Saved output of task 'attempt_20180323114931_0003_m_000000_0' to hdfs://cloudera/tmp/extracted_events/_temporary/0/task_20180323114931_0003_m_000000
18/03/23 11:49:32 INFO SparkHadoopMapRedUtil: attempt_20180323114931_0003_m_000000_0: Committed
18/03/23 11:49:32 INFO Executor: Finished task 0.0 in stage 3.0 (TID 3). 2263 bytes result sent to driver
18/03/23 11:49:32 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 3) in 1235 ms on localhost (executor driver) (1/1)
18/03/23 11:49:32 INFO DAGScheduler: ResultStage 3 (parquet at NativeMethodAccessorImpl.java:0) finished in 1.238 s
18/03/23 11:49:32 INFO DAGScheduler: Job 3 finished: parquet at NativeMethodAccessorImpl.java:0, took 1.274748 s
18/03/23 11:49:32 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
18/03/23 11:49:32 INFO FileFormatWriter: Job null committed.
18/03/23 11:49:32 INFO SparkContext: Invoking stop() from shutdown hook
18/03/23 11:49:32 INFO SparkUI: Stopped Spark web UI at http://172.18.0.5:4040
18/03/23 11:49:33 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/23 11:49:33 INFO MemoryStore: MemoryStore cleared
18/03/23 11:49:33 INFO BlockManager: BlockManager stopped
18/03/23 11:49:33 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/23 11:49:33 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/23 11:49:33 INFO SparkContext: Successfully stopped SparkContext
18/03/23 11:49:33 INFO ShutdownHookManager: Shutdown hook called
18/03/23 11:49:33 INFO ShutdownHookManager: Deleting directory /tmp/spark-b855e396-94f3-43a6-8312-8c8ac4ad48ad
18/03/23 11:49:33 INFO ShutdownHookManager: Deleting directory /tmp/spark-b855e396-94f3-43a6-8312-8c8ac4ad48ad/pyspark-67630029-7a93-40a0-a369-72637980b5bd
```

transform_events.py extracts events from kafka, transforms them and writes them out to HDFS via Spark.

##### Verify that the files have been written to HDFS #####

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/
```

```
Found 3 items
drwxr-xr-x   - root   supergroup          0 2018-03-23 11:49 /tmp/extracted_events
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-03-23 11:47 /tmp/hive
```

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/extracted_events/
```

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-03-23 11:49 /tmp/extracted_events/_SUCCESS
-rw-r--r--   1 root supergroup       1715 2018-03-23 11:49 /tmp/extracted_events/part-00000-598c7aee-616a-4576-b779-1006925bd3c1-c000.snappy.parquet
```

The ls commands returns the HDFS file structure showing that the parquet file has been successfully created.

##### Create a python script for extracting events from kafka and separating them by event type using Spark #####

```bash
vim separate_events.py 
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
    event['Host'] = "moe" # silly change to show it works
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
        .toDF()

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

##### Execute separate_events.py to extract events from kafka and separate them by event type via Spark #####

```bash
$ docker-compose exec spark spark-submit /w205/spark-from-files/separate_events.py
```

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/03/23 11:36:29 INFO SparkContext: Running Spark version 2.2.0
18/03/23 11:36:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/03/23 11:36:30 INFO SparkContext: Submitted application: ExtractEventsJob
18/03/23 11:36:30 INFO SecurityManager: Changing view acls to: root
18/03/23 11:36:30 INFO SecurityManager: Changing modify acls to: root
18/03/23 11:36:30 INFO SecurityManager: Changing view acls groups to: 
18/03/23 11:36:30 INFO SecurityManager: Changing modify acls groups to: 
18/03/23 11:36:30 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
18/03/23 11:36:30 INFO Utils: Successfully started service 'sparkDriver' on port 33931.
18/03/23 11:36:30 INFO SparkEnv: Registering MapOutputTracker
18/03/23 11:36:30 INFO SparkEnv: Registering BlockManagerMaster
18/03/23 11:36:30 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/23 11:36:30 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/23 11:36:30 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-634c5249-fc25-4fc7-ad96-f6dda8f0f244
18/03/23 11:36:30 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
18/03/23 11:36:30 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/23 11:36:31 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/23 11:36:31 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.18.0.6:4040
18/03/23 11:36:31 INFO SparkContext: Added file file:/w205/spark-from-files/separate_events.py at file:/w205/spark-from-files/separate_events.py with timestamp 1521804991393
18/03/23 11:36:31 INFO Utils: Copying /w205/spark-from-files/separate_events.py to /tmp/spark-71c348a3-c055-4e9f-9842-d59d7dd178e6/userFiles-255d6e38-64f3-4c96-a694-e88c5b0117e1/separate_events.py
18/03/23 11:36:31 INFO Executor: Starting executor ID driver on host localhost
18/03/23 11:36:31 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 42467.
18/03/23 11:36:31 INFO NettyBlockTransferService: Server created on 172.18.0.6:42467
18/03/23 11:36:31 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/23 11:36:31 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.18.0.6, 42467, None)
18/03/23 11:36:31 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.6:42467 with 366.3 MB RAM, BlockManagerId(driver, 172.18.0.6, 42467, None)
18/03/23 11:36:31 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.18.0.6, 42467, None)
18/03/23 11:36:31 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.18.0.6, 42467, None)
18/03/23 11:36:32 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/').
18/03/23 11:36:32 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse/'.
18/03/23 11:36:33 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
18/03/23 11:36:35 INFO CatalystSqlParser: Parsing command: string
18/03/23 11:36:35 INFO CatalystSqlParser: Parsing command: string
18/03/23 11:36:36 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0
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

18/03/23 11:36:36 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0
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

18/03/23 11:36:36 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:36:36 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:36:36 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0.
18/03/23 11:36:36 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0
18/03/23 11:36:36 INFO AbstractCoordinator: (Re-)joining group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0
18/03/23 11:36:39 INFO AbstractCoordinator: Successfully joined group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0 with generation 1
18/03/23 11:36:39 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-driver-0
18/03/23 11:36:39 INFO KafkaRelation: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,-2,-1,None)
18/03/23 11:36:40 INFO CodeGenerator: Code generated in 329.797725 ms
18/03/23 11:36:40 INFO CodeGenerator: Code generated in 26.847204 ms
18/03/23 11:36:42 INFO SparkContext: Starting job: runJob at PythonRDD.scala:446
18/03/23 11:36:42 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:446) with 1 output partitions
18/03/23 11:36:42 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:446)
18/03/23 11:36:42 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:36:42 INFO DAGScheduler: Missing parents: List()
18/03/23 11:36:42 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48), which has no missing parents
18/03/23 11:36:42 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 21.4 KB, free 366.3 MB)
18/03/23 11:36:42 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.5 KB, free 366.3 MB)
18/03/23 11:36:42 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.6:42467 (size: 10.5 KB, free: 366.3 MB)
18/03/23 11:36:42 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
18/03/23 11:36:42 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[12] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
18/03/23 11:36:42 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
18/03/23 11:36:42 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:36:42 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
18/03/23 11:36:42 INFO Executor: Fetching file:/w205/spark-from-files/separate_events.py with timestamp 1521804991393
18/03/23 11:36:42 INFO Utils: /w205/spark-from-files/separate_events.py has been previously copied to /tmp/spark-71c348a3-c055-4e9f-9842-d59d7dd178e6/userFiles-255d6e38-64f3-4c96-a694-e88c5b0117e1/separate_events.py
18/03/23 11:36:42 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:42 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:42 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:36:42 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:36:43 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor.
18/03/23 11:36:43 INFO CodeGenerator: Code generated in 36.778095 ms
18/03/23 11:36:43 INFO CodeGenerator: Code generated in 44.753173 ms
18/03/23 11:36:43 INFO CodeGenerator: Code generated in 18.887807 ms
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:44 INFO CodeGenerator: Code generated in 50.667345 ms
18/03/23 11:36:44 INFO PythonRunner: Times: total = 1026, boot = 858, init = 167, finish = 1
18/03/23 11:36:44 INFO PythonRunner: Times: total = 54, boot = 9, init = 44, finish = 1
18/03/23 11:36:44 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2042 bytes result sent to driver
18/03/23 11:36:44 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1823 ms on localhost (executor driver) (1/1)
18/03/23 11:36:44 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/23 11:36:44 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:446) finished in 1.874 s
18/03/23 11:36:44 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 2.218301 s
18/03/23 11:36:45 INFO CodeGenerator: Code generated in 47.559093 ms
18/03/23 11:36:45 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/03/23 11:36:45 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:36:45 INFO DAGScheduler: Final stage: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0)
18/03/23 11:36:45 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:36:45 INFO DAGScheduler: Missing parents: List()
18/03/23 11:36:45 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[19] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:36:45 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 30.3 KB, free 366.2 MB)
18/03/23 11:36:45 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 14.2 KB, free 366.2 MB)
18/03/23 11:36:45 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.6:42467 (size: 14.2 KB, free: 366.3 MB)
18/03/23 11:36:45 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
18/03/23 11:36:45 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[19] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:36:45 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
18/03/23 11:36:45 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:36:45 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
18/03/23 11:36:45 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:45 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:45 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:36:45 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:36:46 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor.
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 INFO PythonRunner: Times: total = 53, boot = -1747, init = 1800, finish = 0
18/03/23 11:36:46 INFO CodeGenerator: Code generated in 24.592365 ms
18/03/23 11:36:46 INFO PythonRunner: Times: total = 29, boot = 9, init = 18, finish = 2
18/03/23 11:36:46 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2173 bytes result sent to driver
18/03/23 11:36:46 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 325 ms on localhost (executor driver) (1/1)
18/03/23 11:36:46 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/23 11:36:46 INFO DAGScheduler: ResultStage 1 (showString at NativeMethodAccessorImpl.java:0) finished in 0.329 s
18/03/23 11:36:46 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.606503 s
18/03/23 11:36:46 INFO CodeGenerator: Code generated in 64.759291 ms
+------+-------------+----+-----------+--------------+--------------------+
|Accept|Cache-Control|Host| User-Agent|    event_type|           timestamp|
+------+-------------+----+-----------+--------------+--------------------+
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:04:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
+------+-------------+----+-----------+--------------+--------------------+

18/03/23 11:36:46 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
18/03/23 11:36:46 INFO DAGScheduler: Got job 2 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/23 11:36:46 INFO DAGScheduler: Final stage: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0)
18/03/23 11:36:46 INFO DAGScheduler: Parents of final stage: List()
18/03/23 11:36:46 INFO DAGScheduler: Missing parents: List()
18/03/23 11:36:46 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[22] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/23 11:36:46 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 30.3 KB, free 366.2 MB)
18/03/23 11:36:46 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 14.1 KB, free 366.2 MB)
18/03/23 11:36:46 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.6:42467 (size: 14.1 KB, free: 366.3 MB)
18/03/23 11:36:46 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1006
18/03/23 11:36:46 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[22] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/23 11:36:46 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks
18/03/23 11:36:46 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
18/03/23 11:36:46 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
18/03/23 11:36:46 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:46 INFO ConsumerConfig: ConsumerConfig values: 
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
	group.id = spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor
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

18/03/23 11:36:46 INFO AppInfoParser: Kafka version : 0.10.0.1
18/03/23 11:36:46 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
18/03/23 11:36:46 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-relation-b88ec813-9b34-4948-b557-46e3cac3579a-executor.
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 WARN CachedKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
18/03/23 11:36:46 INFO PythonRunner: Times: total = 64, boot = -554, init = 617, finish = 1
18/03/23 11:36:46 INFO PythonRunner: Times: total = 55, boot = -562, init = 616, finish = 1
18/03/23 11:36:46 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2171 bytes result sent to driver
18/03/23 11:36:46 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 353 ms on localhost (executor driver) (1/1)
18/03/23 11:36:46 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/03/23 11:36:46 INFO DAGScheduler: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0) finished in 0.356 s
18/03/23 11:36:46 INFO DAGScheduler: Job 2 finished: showString at NativeMethodAccessorImpl.java:0, took 0.392233 s
+------+-------------+----+-----------+----------+--------------------+
|Accept|Cache-Control|Host| User-Agent|event_type|           timestamp|
+------+-------------+----+-----------+----------+--------------------+
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:02:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:03:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:03:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:05:...|
+------+-------------+----+-----------+----------+--------------------+

18/03/23 11:36:46 INFO ContextCleaner: Cleaned accumulator 55
18/03/23 11:36:46 INFO ContextCleaner: Cleaned accumulator 82
18/03/23 11:36:46 INFO ContextCleaner: Cleaned accumulator 80
18/03/23 11:36:46 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 172.18.0.6:42467 in memory (size: 14.1 KB, free: 366.3 MB)
18/03/23 11:36:47 INFO ContextCleaner: Cleaned accumulator 81
18/03/23 11:36:47 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.18.0.6:42467 in memory (size: 10.5 KB, free: 366.3 MB)
18/03/23 11:36:47 INFO ContextCleaner: Cleaned accumulator 54
18/03/23 11:36:47 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.18.0.6:42467 in memory (size: 14.2 KB, free: 366.3 MB)
18/03/23 11:36:47 INFO ContextCleaner: Cleaned accumulator 53
18/03/23 11:36:47 INFO SparkContext: Invoking stop() from shutdown hook
18/03/23 11:36:47 INFO SparkUI: Stopped Spark web UI at http://172.18.0.6:4040
18/03/23 11:36:47 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/23 11:36:47 INFO MemoryStore: MemoryStore cleared
18/03/23 11:36:47 INFO BlockManager: BlockManager stopped
18/03/23 11:36:47 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/23 11:36:47 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/23 11:36:47 INFO SparkContext: Successfully stopped SparkContext
18/03/23 11:36:47 INFO ShutdownHookManager: Shutdown hook called
18/03/23 11:36:47 INFO ShutdownHookManager: Deleting directory /tmp/spark-71c348a3-c055-4e9f-9842-d59d7dd178e6/pyspark-941715cb-2639-4ffe-befb-7fb36c92e736
18/03/23 11:36:47 INFO ShutdownHookManager: Deleting directory /tmp/spark-71c348a3-c055-4e9f-9842-d59d7dd178e6
```

separate_events.py extracts events from kafka and separates them by event type as below via Spark:

###### purchase_sword ######

```
+------+-------------+----+-----------+--------------+--------------------+
|Accept|Cache-Control|Host| User-Agent|    event_type|           timestamp|
+------+-------------+----+-----------+--------------+--------------------+
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:04:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|purchase_sword|2018-03-23 11:05:...|
+------+-------------+----+-----------+--------------+--------------------+
```

###### default ######

```
+------+-------------+----+-----------+----------+--------------------+
|Accept|Cache-Control|Host| User-Agent|event_type|           timestamp|
+------+-------------+----+-----------+----------+--------------------+
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:02:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:03:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:03:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:05:...|
|   */*|     no-cache| moe|curl/7.47.0|   default|2018-03-23 11:05:...|
+------+-------------+----+-----------+----------+--------------------+
```

##### Stop the web server with a control-C #####

```
127.0.0.1 - - [23/Mar/2018 11:05:48] "GET /purchase_a_sword HTTP/1.1" 200 -
^Cscience@w205s7-crook-6:~/w205/activity-10-takoloco
```

The web server process is stopped.

##### Take down the cluster #####

```bash
docker-compose down
```

```
Stopping activity10takoloco_spark_1 ... done
Stopping activity10takoloco_kafka_1 ... done
Stopping activity10takoloco_zookeeper_1 ... done
Stopping activity10takoloco_mids_1 ... done
Stopping activity10takoloco_cloudera_1 ... done
Removing activity10takoloco_spark_1 ... done
Removing activity10takoloco_kafka_1 ... done
Removing activity10takoloco_zookeeper_1 ... done
Removing activity10takoloco_mids_1 ... done
Removing activity10takoloco_cloudera_1 ... done
Removing network activity10takoloco_default
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

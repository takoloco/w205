# Project 3 Setup

- You're a data scientist at a game development company.  
- Your latest mobile game has two events you're interested in tracking: 
- `buy a sword` & `join guild`...
- Each has metadata

## Project 3 Task
- Your task: instrument your API server to catch and analyze these two
event types.
- This task will be spread out over the last four assignments (9-12).

---

# Assignment 09

## Follow the steps we did in class 
- for both the simple flask app and the more complex one.

### Turn in your `/assignment-09-<user-name>/README.md` file. It should include:

#### 1) A summary type explanation of the example. ####
* Copy docker-compose.yaml from the course-content directory
* Modify docker-compose.yaml to use the absolute path /home/science/w205 instead of ~/w205
* Start up the docker cluster
* Create a topic on Kafka
* Create a python web server script using the flask library
* Start up the web server
* Access the web server via the curl command
* Stop the web server with a control-C
* Revise the python script to add publishing to the Kafka topic
* Start up the web server again
* Access the web server via the curl command
* Use kafkacat to consume the messages and confirm they match what you have published in the previous step
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
Creating network "activity09takoloco_default" with the default driver
Creating activity09takoloco_zookeeper_1
Creating activity09takoloco_mids_1
Creating activity09takoloco_kafka_1
```

Start up the Docker cluster defined in the docker-compose.yaml which include the following services:

* zookeeper
* kafka
* mids

##### Create a topic on Kafka #####

```bash
$  docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```
```
Created topic "events".
```

Create a Kafka topic called "events"

##### Create a python web server script using the flask library #####

```bash
vim game_api.py
```

Create a file called game_api.py and save the file to the file system after editing

##### Start up the web server #####

```bash
docker-compose exec mids env FLASK_APP=/w205/flask-with-kafka/game_api.py flask run
```

```
 * Serving Flask app "game_api"
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```

Start up a Flask web server process with the Python file /w205/flask-with-kafka/game_api.py

##### Access the web server via the curl command #####

```bash
docker-compose exec mids curl http://localhost:5000/
```

###### Client ######

```
This is the default response!
```

###### Server ######

```
127.0.0.1 - - [16/Mar/2018 05:10:38] "GET / HTTP/1.1" 200 -
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/". The web server responds with the text sring "This is the default response!".

```bash
docker-compose exec mids curl http://localhost:5000/purchase_a_sword
```

###### Client ######

```
Sword Purchased!
```

###### Server ######

```
127.0.0.1 - - [16/Mar/2018 05:10:58] "GET /purchase_a_sword HTTP/1.1" 200 -
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/purchase_a_sword". The web server responds with the text sring "Sword Purchased!".

##### Stop the web server with a control-C #####

```bash
^Cscience@w205s7-crook-6:~/w205/activity-09-takoloco
```

The web server process is stopped.

##### Revise the python script to add publishing to the Kafka topic #####

```bash
vim game_api.py
```

Revise the file game_api.py and save the file to the file system after editing

##### Start up the web server again #####

```bash
docker-compose exec mids env FLASK_APP=/w205/flask-with-kafka/game_api.py flask run
```

```
 * Serving Flask app "game_api"
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```

Start up a Flask web server process with the Python file /w205/flask-with-kafka/game_api.py

##### Access the web server via the curl command #####

```bash
docker-compose exec mids curl http://localhost:5000/
```

###### Client ######

```
This is the default response!
```

###### Server ######

```
127.0.0.1 - - [16/Mar/2018 05:10:38] "GET / HTTP/1.1" 200 -
```

```bash
docker-compose exec mids curl http://localhost:5000/purchase_a_sword
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/". The web server publishes an appropriate event to the Kafka topic and responds with the text sring "This is the default response!".

###### Client ######

```
Sword Purchased!
```

###### Server ######

```
127.0.0.1 - - [16/Mar/2018 05:10:58] "GET /purchase_a_sword HTTP/1.1" 200 -
```

Make a HTTP GET request against the localhost on the port 5000 at the location "/purchase_a_sword". The web server publishes an appropriate event to the Kafka topic and responds with the text sring "Sword Purchased!!".

##### Use kafkacat to consume the messages and confirm they match what you have published in the previous step #####

```bash
docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t events -o beginning -e"
```

```
default
default
purchased_sword
purchased_sword
default
default
purchased_sword
purchased_sword
default
default
% Reached end of topic events [0] at offset 10: exiting
```

Consumes all the messages published to the Kafka topic "events" 

##### Stop the web server with a control-C #####

```bash
^Cscience@w205s7-crook-6:~/w205/activity-09-takoloco
```

The web server process is stopped.

##### Take down the cluster #####

```bash
docker-compose down
```

```
Stopping activity09takoloco_kafka_1 ... done
Stopping activity09takoloco_mids_1 ... done
Stopping activity09takoloco_zookeeper_1 ... done
Removing activity09takoloco_kafka_1 ... done
Removing activity09takoloco_mids_1 ... done
Removing activity09takoloco_zookeeper_1 ... done
Removing network activity09takoloco_default
```

The docker cluster and all of its services defined in docker-compose.yaml are stopped. 

# My annotations, Assignment 7 #
## Move into the w205 directory ##
   41  cd w205
   42  ls
## Clone the Activity 7 repo ##
   43  git clone https://github.com/mids-w205-crook/activity-07-takoloco.git
   44  ls
   45  cd activity-07-takoloco/
## Copy docker-compose.yml from the synch session working directory to the current working directory  ##
   46  cp -ip ../spark-with-kafka/docker-compose.yml .
## Start up the cluster ##
   47  docker-compose up -d
   48  cd ..
   49  ls
## Download a JSON file from the specified location with the curl command ##
   50  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4
   51  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '. | length'"
   52  ls
   53  cd activity-07-takoloco/
## Count how many messagees there are in the downloaded JSON file ##
   54  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '. | length'"
## Publish data ##
   55  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t foo && echo 'Produced 3280 messages.'"
## Process half of the published data (1640 messages) ##
   56  docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic foo --from-beginning --max-messages 1640
## Process the remaining ublished data ##
   57  docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t foo -o 1640 -e" | wc -l
## Taken down the cluster ##
   58  docker-compose down
## Confirm all the docker-compose processes have been shut down properly ##
   59  docker-compose ps
   60  docker ps -a
   61  history

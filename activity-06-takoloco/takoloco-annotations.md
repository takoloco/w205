# My annotations, Assignment 6 # 

## Download a JSON file from the specified location with the curl command ##

  318  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4

## Confirm that the file is indeed a JSON file with actual data rather than an error ##

  319  head -10 assessment-attempts-20180128-121051-nested.json
 
## Move the downloaded JSON file to the directory to be mounted as /w205 in the mids container  ##

  320  mv -i assessment-attempts-20180128-121051-nested.json ../w205-7

## Start up the cluster  ##

  321  docker-compose up -d

## Publish data  ##

  322  docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t foo && echo 'Produced 100 messages.'"

## Take down the cluster ##

  323  docker-compose down

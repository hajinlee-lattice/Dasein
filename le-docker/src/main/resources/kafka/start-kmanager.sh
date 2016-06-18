#!/bin/bash

echo 'stopping running kafka-manager container'
docker stop kafka-mgr 2> /dev/null
docker rm kafka-mgr 2> /dev/null

docker run -d -p 9000:9000 -e ZK_HOSTS="localhost:2181" --name kafka-mgr latticeengines/kafka-manager
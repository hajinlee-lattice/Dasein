#!/bin/bash

echo 'stopping running kafka-manager container'
docker stop kafka-manager 2> /dev/null
docker rm kafka-manager 2> /dev/null

docker run -itd -p 9000:9000 -e ZK_HOSTS="localhost:2181" --name kafka-manager latticeengines/kafka-manager
docker network connect kafka kafka-manager

docker exec kafka-manager /usr/zookeeper/bin/zkServer.sh start
docker exec kafka-manager bash -c 'nohup /opt/kafka-manager/bin/kafka-manager 2>&1 > /tmp/kafka-manager.out &' &
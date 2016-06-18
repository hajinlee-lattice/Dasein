#!/bin/bash

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

ZK=$1
ZK_PORT=$2
NETWORK=$3

if [ -z ${ZK} ]; then
    ZK=zk
fi

if [ -z ${ZK_PORT} ]; then
    ZK_PORT=2181
fi

# cleanup
bash ./teardown.sh

# run centos container
echo "provisioning ${ZK}1"
docker run -d --name ${ZK}1 -l cluster.name=${ZK} -e MY_ID=1 -p ${ZK_PORT}:2181 latticeengines/zookeeper
for i in $(seq 2 $ZK_NODES);
do
	echo "provisioning ${ZK}${i}"
	docker run -d --name ${ZK}$i -e MY_ID=$i -l cluster.name=${ZK} latticeengines/zookeeper
done

TMP_DIR=.${ZK}_tmp

rm -rf $TMP_DIR 2> /dev/null || true
mkdir $TMP_DIR

cp conf/zoo.cfg $TMP_DIR/zoo.cfg

ZK_HOSTS=""
for i in $(seq 1 $ZK_NODES);
do
	CONTAINER_NAME=${ZK}$i

	INTER_IP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" ${CONTAINER_NAME})
	ZK_HOSTS="${ZK_HOSTS},${INTER_IP}:2181"
	echo "server.${i}=${INTER_IP}:2888:3888" >> $TMP_DIR/zoo.cfg
done

ZK_HOSTS=`echo $ZK_HOSTS | cut -d , -f 2-`

for i in $(seq 1 $ZK_NODES);
do
	docker cp $TMP_DIR/zoo.cfg $CONTAINER_NAME:/usr/zookeeper/conf/zoo.cfg
done

rm -rf $TMP_DIR

bash start-zookeeper.sh ${ZK}

sleep 3
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"
echo 'you can use this zk connection: '${ZK_HOSTS}
#!/usr/bin/env bash

echo $MY_ID > /var/lib/zookeeper/myid


if [ -z ${ZK_CLUSTER_SIZE} ]; then
    ZK_CLUSTER_SIZE=3
fi

if [ -z ${ZK_CLUSTER} ]; then
    ZK_CLUSTER=zookeeper
fi

ZK_CONF=/usr/zookeeper/conf/zoo.cfg

for i in $(seq 1 ${ZK_CLUSTER_SIZE});
do
	HOST_NAME=${ZK_CLUSTER}-zk${i}
	SERVER="server.${i}=${HOST_NAME}:2888:3888"
	echo sed -i 's/$SERVER//g' $ZK_CONF
	echo $SERVER >> $ZK_CONF
done

while [ 1 = 1 ];
do
	/usr/zookeeper/bin/zkServer.sh start-foreground || true
	sleep 3
done


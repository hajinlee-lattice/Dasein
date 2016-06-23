#!/usr/bin/env bash

ZK_CONF=/usr/zookeeper/conf/zoo.cfg

if [ -z ${ZK_CLUSTER_SIZE} ]; then
    ZK_CLUSTER_SIZE=3
fi

if [ $ZK_CLUSTER_SIZE = 1 ]; then
    echo 'Spin up a single node zookeeper'
    while [ 1 = 1 ];
    do
        /usr/zookeeper/bin/zkServer.sh start-foreground || true
        sleep 3
    done
fi

if [ -z "${DISCOVER_SERVICE}" ]; then

    echo $MY_ID > /var/lib/zookeeper/myid

    if [ -z ${ZK_HOST_PATTERN} ]; then
        ZK_HOST_PATTERN="${ZK_CLUSTER_NAME}-zk{}"
    fi

    for i in $(seq 1 ${ZK_CLUSTER_SIZE});
    do
        HOST_NAME=`echo $ZK_HOST_PATTERN | sed "s|{}|${i}|g"`
        SERVER="server.${i}=${HOST_NAME}:2888:3888"
        echo sed -i 's/$SERVER//g' $ZK_CONF
        echo $SERVER >> $ZK_CONF
    done

else

    if [ -z "${ZK_CLUSTER_NAME}" ]; then
        echo "Must provide KAFKA_CLUSTER_NAME"
        exit -1
    fi

    if [ -f /etc/internaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/internaladdr.txt`
    fi

    echo "ADVERTISE_IP=${ADVERTISE_IP}"

    if [ -z "${ADVERTISE_IP}" ]; then
        echo "Must put advertis ip in /etc/internaladdr.txt"
        exit -1
    fi

    QUORUM=""
	while [ -z "${QUORUM}" ];
	do

        echo "Attempt to get quorum from external discover service ${DISCOVER_SERVICE}/quorums/${ZK_CLUSTER_NAME}?n=${ZK_CLUSTER_SIZE}&ip=${ADVERTISE_IP}"
        QUORUM=`curl -X GET -m 120 ${DISCOVER_SERVICE}/quorums/${ZK_CLUSTER_NAME}?n=${ZK_CLUSTER_SIZE}\&ip=${ADVERTISE_IP}`

	    echo "Got response \"${QUORUM}\""
	    sleep 3

	    ERROR=`echo $QUORUM | grep "500 Internal Server Error"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        QUORUM=""
	        continue;
	    fi
	    if [ -z "${QUORUM}" ]; then
	        continue
	    fi
	done
	echo "QUORUM=${QUORUM}"

	for SERVER in $QUORUM;
	do
	    echo $SERVER >> $ZK_CONF
	done

    MY_ID=""
    while [ -z "${MY_ID}" ];
	do

        echo "Attempt to get myid from external discover service ${DISCOVER_SERVICE}/quorums/${ZK_CLUSTER_NAME}/myid?ip=${ADVERTISE_IP}"
        MY_ID=`curl -X GET -m 120 ${DISCOVER_SERVICE}/quorums/${ZK_CLUSTER_NAME}/myid?ip=${ADVERTISE_IP}`

	    echo "Got response \"${MY_ID}\""
	    sleep 3

	    ERROR=`echo $MY_ID | grep "500 Internal Server Error"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        MY_ID=""
	        continue;
	    fi
	    if [ -z "${MY_ID}" ]; then
	        continue
	    fi
	done

	echo "MY_ID=${MY_ID}"
    echo $MY_ID > /var/lib/zookeeper/myid
fi

while [ 1 = 1 ];
do
	/usr/zookeeper/bin/zkServer.sh start-foreground || true
	sleep 3
done


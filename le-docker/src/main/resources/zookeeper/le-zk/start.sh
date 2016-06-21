#!/usr/bin/env bash

ZK_CONF=/usr/zookeeper/conf/zoo.cfg

if [ -z ${ZK_CLUSTER_SIZE} ]; then
    ZK_CLUSTER_SIZE=3
fi

if [ -z "${DISCOVER_SERVICE}" ]; then

    echo $MY_ID > /var/lib/zookeeper/myid

    if [ -z ${ZK_HOST_PATTERN} ]; then
        ZK_HOST_PATTERN="${ZK_CLUSTER}-zk{}"
    fi

    for i in $(seq 1 ${ZK_CLUSTER_SIZE});
    do
        HOST_NAME=`sed "s|{}|${i}|g" $ZK_HOST_PATTERN`
        SERVER="server.${i}=${HOST_NAME}:2888:3888"
        echo sed -i 's/$SERVER//g' $ZK_CONF
        echo $SERVER >> $ZK_CONF
    done

else

    if [ -z "${ZK_CLUSTER_NAME}" ]; then
        echo "Must provide KAFKA_CLUSTER_NAME"
        exit -1
    fi

    if [ "${RETRIEVE_INTERNAL_ADDR}" == "true" ]; then

        SERVICE_ADDR=""
        while [ -z "${SERVICE_ADDR}" ];
        do
            echo "Attempt to get quorum from external discover service ${DISCOVER_SERVICE}/internal_addr"
            SERVICE_ADDR=`curl -X GET ${DISCOVER_SERVICE}/internal_addr`
            echo "Got response \"${SERVICE_ADDR}\""
            sleep 3

            ERROR=`echo $SERVICE_ADDR | grep "500 Internal Server Error"`
            if [ -z "${ERROR}" ]; then
                echo "Great! there is no error."
            else
                echo "Error:\n${ERROR}"
                SERVICE_ADDR=""
                continue;
            fi
            if [ -z "${SERVICE_ADDR}" ]; then
                continue
            fi
        done
        echo "SERVICE_ADDR=${SERVICE_ADDR}"

    else

        SERVICE_ADDR=$DISCOVER_SERVICE

    fi

    echo using service address $SERVICE_ADDR

    QUORUM=""
	while [ -z "${QUORUM}" ];
	do
	    echo "Attempt to get quorum from external discover service ${SERVICE_ADDR}/quorums/${ZK_CLUSTER_NAME}?n=${ZK_CLUSTER_SIZE}"
	    QUORUM=`curl -X GET ${SERVICE_ADDR}/quorums/${ZK_CLUSTER_NAME}?n=${ZK_CLUSTER_SIZE}`
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
	    echo "Attempt to get myid from external discover service ${SERVICE_ADDR}/quorums/${ZK_CLUSTER_NAME}/myid"
	    MY_ID=`curl -X GET ${SERVICE_ADDR}/quorums/${ZK_CLUSTER_NAME}/myid`
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


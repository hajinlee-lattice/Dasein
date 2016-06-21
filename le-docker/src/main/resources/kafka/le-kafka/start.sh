#!/usr/bin/env bash

if [ -z "${DISCOVER_SERVICE}" ];
then
    if [ -z "${ADVERTISE_IP}" ]; then
        echo "Must provide either ADVERTISE_IP or DISCOVER_SERVICE!"
        exit -1
    fi
    if [ -z "${ZK_HOSTS}" ]; then
        echo "Must provide either ZK_HOSTS or DISCOVER_SERVICE!"
        exit -1
    fi

else
    if [ -z "${KAFKA_CLUSTER_NAME}" ]; then
        echo "Must provide KAFKA_CLUSTER_NAME!"
        exit -1
    fi

    ADVERTISE_IP=""
    while [ -z "${ADVERTISE_IP}" ];
	do
	    echo "Attempt to get advertiser ip from external discover service ${DISCOVER_SERVICE}"
	    ADVERTISE_IP=`curl -X GET ${DISCOVER_SERVICE}/advertiseip`
	    echo "Got response \"${ADVERTISE_IP}\""
	    sleep 3

	    ERROR=`echo $ADVERTISE_IP | grep "DOCTYPE HTML"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        ADVERTISE_IP=""
	        continue;
	    fi
	    if [ -z "${ADVERTISE_IP}" ]; then
	        continue
	    fi
	done
	echo "ADVERTISE_IP=${ADVERTISE_IP}"

	ZK_HOSTS=""
    while [ -z "${ZK_HOSTS}" ];
	do
	    echo "Attempt to get ZK hosts from external discover service ${ZK_HOSTS}"
	    ZK_HOSTS=`curl -X GET ${DISCOVER_SERVICE}/quorums/${KAFKA_CLUSTER_NAME}/zkhosts`
	    echo "Got response \"${ZK_HOSTS}\""
	    sleep 3

	    ERROR=`echo $ZK_HOSTS | grep "DOCTYPE HTML"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        ZK_HOSTS=""
	        continue;
	    fi
	    if [ -z "${ZK_HOSTS}" ]; then
	        continue
	    fi
	done
	echo "ZK_HOSTS=${ZK_HOSTS}"

fi

if [ -z "${BROKER_PORT}" ]; then
    BROKER_PORT=9092
fi

sed -i "s|{{BROKER_PORT}}|$BROKER_PORT|g" /etc/kafka/server.properties
sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka/server.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/kafka/server.properties

JMX_PORT=9199 /usr/bin/kafka-server-start /etc/kafka/server.properties

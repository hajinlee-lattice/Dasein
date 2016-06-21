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

    ADVERTISE_IP=""
    while [ -z "${ADVERTISE_IP}" ];
	do
	    echo "Attempt to get advertiser ip from external discover service ${SERVICE_ADDR}/advertiseip"
	    ADVERTISE_IP=`curl -X GET ${SERVICE_ADDR}/advertiseip`
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

	while [ -z "${ZK_HOSTS}" ];
	do
	    echo "Attempt to get ZK hosts from external discover service ${SERVICE_ADDR}/quorums/${KAFKA_CLUSTER_NAME}/zkhosts"
	    ZK_HOSTS=`curl -X GET ${SERVICE_ADDR}/quorums/${KAFKA_CLUSTER_NAME}/zkhosts`
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

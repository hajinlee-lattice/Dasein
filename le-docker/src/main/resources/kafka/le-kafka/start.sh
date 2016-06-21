#!/usr/bin/env bash

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -z "${DISCOVER_SERVICE}" ]; then
        echo "Must provid either ADVERTISE_IP or DISCOVER_SERVICE!"
        exit -1
    fi
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
fi

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka/server.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/kafka/server.properties

JMX_PORT=9199 /usr/bin/kafka-server-start /etc/kafka/server.properties

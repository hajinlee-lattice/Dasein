#!/usr/bin/env bash

if [ -z "${DISCOVER_SERVICE}" ];
then
    if [ -z "${ADVERTISE_IP}" ]; then
        echo "Must provid either ADVERTISE_IP or DISCOVER_SERVICE!"
        exit -1
    fi
    if [ -z "${ZK_HOSTS}" ]; then
        echo "Must provid either ZK_HOSTS or DISCOVER_SERVICE!"
        exit -1
    fi

else
    ADVERTISE_IP=""
    while [ -z "${ADVERTISE_IP}" ];
	do
	    echo "Attempt to get ZK hosts from external discover service ${DISCOVER_SERVICE}"
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
	    echo "Attempt to get advertiser ip from external discover service ${ZK_HOSTS}"
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

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/schema-registry/schema-registry.properties

while [ 1 == 1 ]; do
	/usr/bin/schema-registry-start /etc/schema-registry/schema-registry.properties
	sleep 3
done

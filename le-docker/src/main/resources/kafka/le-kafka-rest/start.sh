#!/usr/bin/env bash

sed -i "s|{{CONSUMER_ID}}|$HOSTNAME-$(date +%s)|g" /etc/kafka-rest/kafka-rest.properties

if [ -z "${DISCOVER_SERVICE}" ];
then
    if [ -z "${ZK_HOSTS}" ]; then
        echo "Must provide either ZK_HOSTS or DISCOVER_SERVICE!"
        exit -1
    fi
else
    if [ -z "${KAFKA_CLUSTER_NAME}" ]; then
        echo "Must provide KAFKA_CLUSTER_NAME!"
        exit -1
    fi

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

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{SR_PROXY}}|$SR_PROXY|g" /etc/kafka-rest/kafka-rest.properties

while [ 1 == 1 ]; do
	/usr/bin/kafka-rest-start /etc/kafka-rest/kafka-rest.properties
	sleep 3
done


#!/usr/bin/env bash

function valid_ip()
{
    local  ip=$1
    local  stat=1

    if [[ $ip =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        OIFS=$IFS
        IFS='.'
        ip=($ip)
        IFS=$OIFS
        [[ ${ip[0]} -le 255 && ${ip[1]} -le 255 \
            && ${ip[2]} -le 255 && ${ip[3]} -le 255 ]]
        stat=$?
    fi
    return $stat
}

# on AWS we save public and private ip on the EC2 instance

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -f /etc/externaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/externaladdr.txt`
    fi
fi

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -f /etc/internaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/internaladdr.txt`
    fi
fi

# on local we pass in host name, and here we resolve it to ip
if ! valid_ip ${ADVERTISE_IP} ; then
    ADVERTISE_IP=$(dig +short "${ADVERTISE_IP}")
fi

LOG_DIRS="${LOG_DIRS:=/var/lib/kafka}"
RACK_ID="${RACK_ID:=1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:=3}"
NUM_PARTITIONS="${NUM_PARTITIONS:=16}"

if [  ${SINGLE_NODE} == "true" ]; then
    REPLICATION_FACTOR=1
    NUM_PARTITIONS=2
fi

sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/kafka/server.properties
sed -i "s|{{RACK_ID}}|$RACK_ID|g" /etc/kafka/server.properties
sed -i "s|{{LOG_DIRS}}|$LOG_DIRS|g" /etc/kafka/server.properties
sed -i "s|{{NUM_PARTITIONS}}|$NUM_PARTITIONS|g" /etc/kafka/server.properties
sed -i "s|{{REPLICATION_FACTOR}}|$REPLICATION_FACTOR|g" /etc/kafka/server.properties
sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka/server.properties

if [  ${SINGLE_NODE} == "true" ]; then
    sed -i "s|default.replication.factor=2|default.replication.factor=1|g" /etc/kafka/server.properties
fi

cat /etc/kafka/server.properties

JMX_PORT=9199 /usr/bin/kafka-server-start /etc/kafka/server.properties

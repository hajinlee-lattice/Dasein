#!/usr/bin/env bash

if [ -z $KAFKA_CLUSTER_NAME ]; then
	KAFKA_CLUSTER_NAME=kafka
fi

if [ -z $SR_SERVER_PATTERN ]; then
	SR_SERVER_PATTERN=${KAFKA_CLUSTER_NAME}-sr{i}
fi

if [ -z $KR_SERVER_PATTERN ]; then
	KR_SERVER_PATTERN=${KAFKA_CLUSTER_NAME}-rest{i}
fi

if [ -z $NUM_SR_SERVER ]; then
	NUM_SR_SERVER=2
fi

if [ -z $NUM_KR_SERVER ]; then
	NUM_KR_SERVER=2
fi

SR_HOSTS=""
for i in $(seq 1 $NUM_SR_SERVER);
do
	SERVER_NAME=$(echo $SR_SERVER_PATTERN | sed "s|{i}|${i}|g")

	while [ 1 == 1 ]; do
		SERVER_IP=$(dig +short $SERVER_NAME)
		if [ -z SERVER_IP ]; then
			continue
		else
			break
		fi
	done

	SR_HOSTS="${SR_HOSTS}\n    server ${SERVER_NAME} ${SERVER_IP}:9022 check"
done

KR_HOSTS=""
for i in $(seq 1 $NUM_KR_SERVER);
do
	SERVER_NAME=$(echo $KR_SERVER_PATTERN | sed "s|{i}|${i}|g")

	while [ 1 == 1 ]; do
		SERVER_IP=$(dig +short $SERVER_NAME)
		if [ -z SERVER_IP ]; then
			continue
		else
			break
		fi
	done

	KR_HOSTS="${KR_HOSTS}\n    server ${SERVER_NAME} ${SERVER_IP}:9023 check"
done

sed -i "s/{{SR_HOSTS}}/$SR_HOSTS/g" /usr/local/etc/haproxy/haproxy.cfg
sed -i "s/{{KR_HOSTS}}/$KR_HOSTS/g" /usr/local/etc/haproxy/haproxy.cfg

/docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg

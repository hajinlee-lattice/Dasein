#!/usr/bin/env bash

source functions.sh
verify_consul

CLUSTER=$1
NETWORK=$2

CLUSTER="${CLUSTER:=hdp}"
NETWORK="${NETWORK:=lenet}"

teardown_hdp ${CLUSTER}

docker run -d \
    --net ${NETWORK} \
    --name ${CLUSTER}_hdp \
    -h ${CLUSTER}-hdp \
    -p 9000:9000 \
    -p 50070:50070 \
    -p 50010:50010 \
    -p 8088:8088 \
    -p 8188:8188 \
    -p 10200:10200 \
    -p 19888:19888 \
    -e NAMENODE=${CLUSTER}-hdp \
    -l hdp.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    latticeengines/hdp

sleep 2
docker_ps

register_service "hdp" ${CLUSTER}_hdp 9000
show_service "hdp"

DATANODE_IP=$(python ../consul.py ip -s hdp)

echo "adding hdp ip ${DATANODE_IP} to /etc/hosts"
sudo sed -i "s/hdp//g" /etc/hosts
sudo chmod a+w /etc/hosts
echo "${DATANODE_IP}      ${CLUSTER}-hdp" >> /etc/hosts

echo "sleep 10 second for cluster to warm up"
sleep 10
echo "uploading tez tar ball .."
docker exec ${CLUSTER}_hdp bash -c "bash /upload_tez.sh"
echo "check uploaded tez"
hdfs dfs -ls /apps/tez
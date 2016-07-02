#!/usr/bin/env bash

bash ./teardown.sh

docker run -d --name hdp \
    -h hdp \
    -p 9000:9000 \
    -p 50070:50070 \
    -p 50010:50010 \
    -p 8088:8088 \
    -p 8188:8188 \
    -p 10200:10200 \
    -p 19888:19888 \
    -e NAMENODE=hdp \
    latticeengines/hdp

DATANODE_IP=$(docker inspect --format="{{.NetworkSettings.IPAddress}}" hdp)

echo "adding hdp ip ${DATANODE_IP} to /etc/hosts"
sudo sed -i "s/hdp//g" /etc/hosts
sudo chmod a+w /etc/hosts
echo "${DATANODE_IP}      hdp" >> /etc/hosts

echo "sleep 10 second for cluster to warm up"
sleep 10
echo "uploading .tez tar ball .."
docker exec hdp bash -c "bash /upload_tez.sh"
echo "check uploaded tez"
hdfs dfs -ls /apps/tez
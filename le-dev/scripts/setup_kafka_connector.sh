I#!/usr/bin/env bash

function printUsage
{
    echo "Usage: setup_kafka.sh dev|devcluster|qacluster|prodcluster"
    exit 0
}

if [ "$#" -ne 1 ]; then
    printUsage
fi
printf "%s$s" "It's env:" $1

topic_env=$1
topic_stack=global
repl_factor=1
rest_port=8081
partitions=100
hadoop_conf_dir=/usr/hdp/current/hadoop-client/etc/hadoop

if [ $1 == dev ]; then
    hdfs_pod=Default
    zk_servers=127.0.0.1:2181
    camille_zk_servers=127.0.0.1:2181
    camille_zk_pod_id=Default
    hadoop_conf_dir= 
    rest_port=9022
    partitions=24
elif [ $1 == devcluster ]; then
    hdfs_pod=QA
    zk_servers=10.41.1.116:2181,10.41.1.137:2181,10.41.1.138:2181
    camille_zk_servers=qazklayer1.lattice.local,qazklayer4.lattice.local,qazklayer5.lattice.local
    camille_zk_pod_id=QA
elif [ $1 == qacluster ]; then
    hdfs_pod=QA
    zk_servers=10.41.1.116:2181,10.41.1.137:2181,10.41.1.138:2181
    camille_zk_servers=qazklayer1.lattice.local,qazklayer4.lattice.local,qazklayer5.lattice.local
    camille_zk_pod_id=QA
    repl_factor=3
elif [ $1 == prodcluster ]; then
    hdfs_pod=Production
    zk_servers=10.51.1.78:2181,10.51.1.79:2181,10.51.1.114:2181
    camille_zk_servers=zklayer4.prod.lattice.local,zklayer5.prod.lattice.local,zklayer6.prod.lattice.local
    camille_zk_pod_id=Production
    repl_factor=3
else 
    printUsage
fi
    
# setup topic
topic_name=Env_${topic_env}_Stack_${topic_stack}_FabricGenericConnector

topic_names=`kafka-topics --list --zookeeper 127.0.0.1:2181`
found_topic=false
for topic in $topic_names 
do
    if [ $topic == "$topic_name" ]; then
        found_topic=true
        break;
    fi
done

if [ $found_topic == false ]; then
    kafka-topics --zookeeper 127.0.0.1:2181 --create --topic $topic_name --partitions ${partitions} --replication-factor $repl_factor 2> /tmp/errors.txt
else
    kafka-topics --zookeeper 127.0.0.1:2181 --alter --partitions ${partitions} --topic $topic_name 2> /tmp/errors.txt
fi
echo 

rest_url="http://localhost:${rest_port}/config/${topic_name}-key"
curl -X PUT $rest_url -H "Content-type: Application/json" -d '{"compatibility": "NONE"}'
echo

rest_url="http://localhost:${rest_port}/config/${topic_name}-value"
curl -X PUT $rest_url -H "Content-type: Application/json" -d '{"compatibility": "NONE"}'
echo

# setup connector
rest_url="http://localhost:8083/connectors"
curl -X POST $rest_url -H "Content-type: Application/json" -d '{"name": "generic-sink-connector-'${topic_env}'", "config": {"connector.class": "com.latticeengines.datafabric.connector.generic.GenericSinkConnector","tasks.max": "'${partitions}'","topics": "'${topic_name}'"}}'  2>> /tmp/errors.txt
echo

rest_url="http://localhost:8083/connectors/generic-sink-connector-${topic_env}/config"
echo curl -X PUT $rest_url -H "Content-type: Application/json" -d '{"name": "generic-sink-connector-'${topic_env}'","connector.class": "com.latticeengines.datafabric.connector.generic.GenericSinkConnector","tasks.max": "'${partitions}'","topics": "'${topic_name}'", "hdfs.base.dir" : "/Pods/'${hdfs_pod}'/Services/PropData/Sources", "hadoop.conf.dir":"'${hadoop_conf_dir}'", "camille.zk.connectionString":"'${camille_zk_servers}'", "camille.zk.pod.id":"'${camille_zk_pod_id}'", "kafka.zkConnect" : "'${zk_servers}'"}'  2>> /tmp/errors.txt
curl -X PUT $rest_url -H "Content-type: Application/json" -d '{"name": "generic-sink-connector-'${topic_env}'","connector.class": "com.latticeengines.datafabric.connector.generic.GenericSinkConnector","tasks.max": "'${partitions}'","topics": "'${topic_name}'", "hdfs.base.dir" : "/Pods/'${hdfs_pod}'/Services/PropData/Sources", "hadoop.conf.dir":"'${hadoop_conf_dir}'", "camille.zk.connectionString":"'${camille_zk_servers}'", "camille.zk.pod.id":"'${camille_zk_pod_id}'", "kafka.zkConnect" : "'${zk_servers}'"}'  2>> /tmp/errors.txt
echo


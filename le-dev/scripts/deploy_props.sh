#!/bin/bash

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source ${WSHOME}/le-dev/aliases

CLUSTER_NAME=$(cat ${WSHOME}/le-config/conf/env/devcluster/latticeengines.properties | grep aws.emr.cluster | cut -d = -f 2)
echo "From properties file, find emr cluster name: ${CLUSTER_NAME}"

STACK_NAME=${STACK_NAME:=${LE_STACK}}
echo "Assuming stack name is ${STACK_NAME}"

if [[ -z "${STACK_IP}" ]]; then
    STACK_IP=$(aws ec2 describe-instances --filter "Name=tag:Name,Values=${STACK_NAME}" Name=instance-state-name,Values=running --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)
    echo "Found the ip or mini-stack ${STACK_NAME} is [${STACK_IP}]"
else
    echo "Use given mini-stack ip ${STACK_IP}"
fi

if [[ ${STACK_IP} == "None" ]] || [[ ${STACK_IP} == "" ]]; then
    echo "Stack IP [${STACK_IP}] is invalid, check your mini-stack provision!"
    exit -1
fi

#Query to get the Cluster ID based on Cluster Name
CLUSTER_ID=$(aws emr list-clusters --region us-east-1 --query 'Clusters[?Name==`'${CLUSTER_NAME}'`].Id' --cluster-states RUNNING WAITING --output text)
#Query to get the Master Node IP based on the Cluster ID
EMR_HOST_NAME=$(aws emr list-instances --region us-east-1 --cluster-id ${CLUSTER_ID} --instance-group-types MASTER | jq -r .Instances[0].PrivateIpAddress)
echo "Find emr master ip: ${EMR_HOST_NAME}"

PROP_FILE=${WSHOME}/le-config/conf/env/devcluster/latticeengines.properties
if [[ $(uname) == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' 's/${LE_STACK}/'${STACK_NAME}'/g' ${PROP_FILE}
    sed -i '' 's/${LE_CLIENT_ADDRESS}/'${STACK_IP}'/g' ${PROP_FILE}
else
    echo "You are on ${UNAME}"
    sed -i 's/${LE_STACK}/'${STACK_NAME}'/g' ${PROP_FILE}
    sed -i 's/${LE_CLIENT_ADDRESS}/'${STACK_IP}'/g' ${PROP_FILE}
fi

ssh -t ${STACK_IP} "sudo mkdir -p /etc/ledp && sudo chmod 777 /etc/ledp"
for f in "latticeengines.properties" "log4j.properties" "log4j2-yarn.xml"; do
    F_PATH=${WSHOME}/le-config/conf/env/devcluster/${f}
    hdfs dfs -put -f ${F_PATH} hdfs://${EMR_HOST_NAME}/app/${STACK_NAME}/$(leversion)/conf
    scp ${F_PATH} ${STACK_IP}:/tmp
    ssh -t ${STACK_IP} "sudo mv -f /tmp/${f} /etc/ledp/${f} && chmod 777 /etc/ledp/${f}"
done

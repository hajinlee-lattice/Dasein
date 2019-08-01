#!/bin/bash

CLUSTER_NAME=$(cat ${WSHOME}/le-config/conf/env/devcluster/latticeengines.properties | grep aws.emr.cluster | cut -d = -f 2)
echo "From properties file, find emr cluster name: ${CLUSTER_NAME}"

STACK_NAME=${STACK_NAME:=${LE_STACK}}
echo "Assuming stack name is ${STACK_NAME}"

STACK_IP=$(aws ec2 describe-instances --filter "Name=tag:Name,Values=${STACK_NAME}" Name=instance-state-name,Values=running --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)

#Query to get the Cluster ID based on Cluster Name
CLUSTER_ID=$(aws emr list-clusters --region us-east-1 --query 'Clusters[?Name==`'${CLUSTER_NAME}'`].Id' --cluster-states RUNNING WAITING --output text)
#Query to get the Master Node IP based on the Cluster ID
EMR_HOST_NAME=$(aws emr list-instances --region us-east-1 --cluster-id ${CLUSTER_ID} --instance-group-types MASTER | jq -r .Instances[0].PrivateIpAddress)

echo "My mini stack name: ${STACK_NAME}, IP=${STACK_IP}"
echo "My emr cluster name: ${CLUSTER_NAME}, IP=${EMR_HOST_NAME}"

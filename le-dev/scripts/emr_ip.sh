#!/usr/bin/env bash

CLUSTER_NAME=$1
PROFILE=$2
PROFILE=${PROFILE:=default}

if [ -z "${CLUSTER_NAME}" ]; then
    echo "usage: emr-ip CLUSTER_NAME"
    exit 0
fi

aws ec2 describe-instances \
    --filters \
        "Name=tag:emr_cluster_name,Values=${CLUSTER_NAME}" \
        "Name=tag:aws:elasticmapreduce:instance-group-role,Values=MASTER" \
        "Name=instance-state-name,Values=running" \
    --query 'Reservations[*].Instances[*].NetworkInterfaces[*].PrivateIpAddresses[*].PrivateIpAddress[]' \
    --profile=${PROFILE} \
    --output text

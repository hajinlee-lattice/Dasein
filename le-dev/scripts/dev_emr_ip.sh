#!/usr/bin/env bash

clusters=$(aws emr list-clusters --region us-east-1 --query 'Clusters[*].Name' --cluster-states RUNNING WAITING --output text)
for cluster in ${clusters}; do
    if [[ ${cluster} == 'devcluster'* ]]; then
        ip=$(aws ec2 describe-instances --filters "Name=tag:emr_cluster_name,Values=${cluster}" "Name=tag:aws:elasticmapreduce:instance-group-role,Values=MASTER" "Name=instance-state-name,Values=running" --query 'Reservations[*].Instances[*].NetworkInterfaces[*].PrivateIpAddresses[*].PrivateIpAddress[]' --output text)
        echo "Name = ${cluster}    MasterIp = ${ip}"
    fi
done



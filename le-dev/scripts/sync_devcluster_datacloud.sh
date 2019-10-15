#!/usr/bin/env bash

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

master_ip=$(emr-ip devcluster_20181019)
echo ${master_ip}

if [ ! -z "${master_ip}" ]; then
    hdfs dfs -rm -r -f hdfs://${master_ip}/Pods/Default/Services/PropData/Sources || true
    hadoop distcp /Pods/Default/Services/PropData/Sources hdfs://${master_ip}/Pods/Default/Services/PropData/Sources
fi

# hadoop distcp hdfs://10.51.11.51/Pods/Production/Services/PropData/Sources/AccountMasterEnrichmentStats hdfs://10.141.11.81/Pods/QA/Services/PropData/Sources/AccountMasterEnrichmentStats

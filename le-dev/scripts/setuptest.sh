#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

hdfs dfs -mkdir -p /tmp/Stoplist || true
hdfs dfs -mkdir -p /tmp/AccountMaster || true
hdfs dfs -mkdir -p /Pods/Default/Services/PropData/MatchService/PublicDomain || true
hdfs dfs -mkdir -p /Pods/Default/Services/ModelQuality/datasets || true
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/Stoplist/Stoplist.avro /tmp/Stoplist
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/PublicDomain/PublicDomain.avro /Pods/Default/Services/PropData/MatchService/PublicDomain
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/PublicDomain/PublicDomain.csv /Pods/Default/Services/PropData/MatchService/PublicDomain
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/AccountMaster/AccountMaster.avro /tmp/AccountMaster
hdfs dfs -put -f $WSHOME/le-modelquality/src/test/resources/com/latticeengines/modelquality/csvfiles/Mulesoft_NA_domain_enhanced21k.csv /Pods/Default/Services/ModelQuality/datasets
hdfs dfs -put -f $WSHOME/le-modelquality/src/test/resources/com/latticeengines/modelquality/csvfiles/NGINXReducedRowsEnhanced_20160712.csv /Pods/Default/Services/ModelQuality/datasets


# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases
#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

hdfs dfs -mkdir -p /tmp/Stoplist || true
hdfs dfs -mkdir -p /tmp/AccountMaster || true
hdfs dfs -mkdir -p /Pods/Default/Services/PropData/MatchService/PublicDomain || true
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/Stoplist/Stoplist.avro /tmp/Stoplist
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/PublicDomain/PublicDomain.avro /Pods/Default/Services/PropData/MatchService/PublicDomain
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/PublicDomain/PublicDomain.csv /Pods/Default/Services/PropData/MatchService/PublicDomain
hdfs dfs -put -f $WSHOME/le-dev/testartifacts/AccountMaster/AccountMaster.avro /tmp/AccountMaster

hdfs dfs -mkdir -p /Pods/Default/Services/ModelQuality
hdfs dfs -put -f $WSHOME/le-pls/src/test/resources/com/latticeengines/pls/end2end/selfServiceModeling/csvfiles/Mulesoft_Migration_LP3_ModelingLead_ReducedRows_20160624_155355.csv /Pods/Default/Services/ModelQuality/

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases
#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

hadoop fs -mkdir /tmp/Stoplist || true
hadoop fs -mkdir /tmp/AccountMaster || true
hadoop fs -copyFromLocal $WSHOME/le-dev/testartifacts/Stoplist/Stoplist.avro /tmp/Stoplist
hadoop fs -copyFromLocal $WSHOME/le-dev/testartifacts/AccountMaster/AccountMaster.avro /tmp/AccountMaster

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

# Create default ZK pod
createpod
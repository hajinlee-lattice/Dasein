#!/bin/bash

hadoop fs -mkdir /tmp/Stoplist || true
hadoop fs -mkdir /tmp/AccountMaster || true
hadoop fs -copyFromLocal $WSHOME/le-dev/testartifacts/Stoplist/Stoplist.avro /tmp/Stoplist
hadoop fs -copyFromLocal $WSHOME/le-dev/testartifacts/AccountMaster/AccountMaster.avro /tmp/AccountMaster
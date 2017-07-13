#!/usr/bin/env bash

hdfs dfs -rm -r -f hdfs://10.41.1.148/Pods/Default/Services/PropData/Sources || true
hadoop distcp /Pods/Default/Services/PropData/Sources hdfs://10.41.1.148/Pods/Default/Services/PropData/Sources
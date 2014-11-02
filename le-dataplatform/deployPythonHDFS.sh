#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -f -r /app
$HADOOP_HOME/bin/hadoop fs -mkdir /app

rm -rf /tmp/app
mkdir -p /tmp/app/dataplatform/scripts/algorithm

cp ~/workspace/ledp/le-dataplatform/conf/env/dev/dataplatform.properties /tmp/app/dataplatform/
cp ~/workspace/ledp/le-dataplatform/conf/env/dev/hadoop-metrics2.properties /tmp/app/dataplatform/
cp ~/workspace/ledp/le-dataplatform/target/leframework.tar.gz /tmp/app/dataplatform/scripts
cp ~/workspace/ledp/le-dataplatform/target/lepipeline.tar.gz /tmp/app/dataplatform/scripts
cp ~/workspace/ledp/le-dataplatform/src/main/python/launcher.py /tmp/app/dataplatform/scripts 
cp ~/workspace/ledp/le-dataplatform/src/main/python/pipelinefwk.py /tmp/app/dataplatform/scripts 
cp ~/workspace/ledp/le-dataplatform/src/main/python/pipeline/pipeline.py /tmp/app/dataplatform/scripts 
cp ~/workspace/ledp/le-dataplatform/src/main/python/algorithm/*.py /tmp/app/dataplatform/scripts/algorithm

$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/app/dataplatform /app/dataplatform

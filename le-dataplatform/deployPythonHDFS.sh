#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -f -r /app
$HADOOP_HOME/bin/hadoop fs -mkdir /app

rm -rf /tmp/app
mkdir -p /tmp/app/dataplatform/scripts/algorithm

cp conf/env/dev/dataplatform.properties /tmp/app/dataplatform/
cp conf/env/dev/hadoop-metrics2.properties /tmp/app/dataplatform/
cp target/leframework.tar.gz /tmp/app/dataplatform/scripts
cp target/lepipeline.tar.gz /tmp/app/dataplatform/scripts
cp src/main/python/launcher.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipelinefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipeline/pipeline.py /tmp/app/dataplatform/scripts 
cp src/main/python/algorithm/*.py /tmp/app/dataplatform/scripts/algorithm

$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/app/dataplatform /app/dataplatform

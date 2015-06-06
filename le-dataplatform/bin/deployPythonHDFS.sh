#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -f -r /app/dataplatform
$HADOOP_HOME/bin/hadoop fs -rm -f -r /app/scoring
$HADOOP_HOME/bin/hadoop fs -mkdir /app/dataplatform
$HADOOP_HOME/bin/hadoop fs -mkdir /app/scoring
$HADOOP_HOME/bin/hadoop fs -rm -f -r /datascientist
$HADOOP_HOME/bin/hadoop fs -mkdir /datascientist

rm -rf /tmp/app
rm -rf /tmp/datascientist
mkdir -p /tmp/app/dataplatform/scripts/algorithm
mkdir -p /tmp/app/dataplatform/eai
mkdir -p /tmp/app/scoring/scripts
mkdir -p /tmp/datascientist

cp conf/env/dev/dataplatform.properties /tmp/app/dataplatform/
cp conf/env/dev/hadoop-metrics2.properties /tmp/app/dataplatform/
cp target/leframework.tar.gz /tmp/app/dataplatform/scripts
cp target/lepipeline.tar.gz /tmp/app/dataplatform/scripts
cp src/main/python/launcher.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipelinefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipeline/pipeline.py /tmp/app/dataplatform/scripts 
cp src/main/python/algorithm/*.py /tmp/app/dataplatform/scripts/algorithm
cp src/test/resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py /tmp/datascientist
cp ../le-eai/conf/env/dev/eai.properties /tmp/app/dataplatform/eai
cp ../le-scoring/src/main/python/scoring.py /tmp/app/scoring/scripts

$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/app /
$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/datascientist /

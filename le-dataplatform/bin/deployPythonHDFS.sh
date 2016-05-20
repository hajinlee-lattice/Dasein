#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -f -r /app/dataplatform
$HADOOP_HOME/bin/hadoop fs -mkdir /app/dataplatform
$HADOOP_HOME/bin/hadoop fs -rm -f -r /app/playmaker
$HADOOP_HOME/bin/hadoop fs -mkdir -p /app/playmaker/evmodel

$HADOOP_HOME/bin/hadoop fs -rm -f -r /datascientist
$HADOOP_HOME/bin/hadoop fs -mkdir /datascientist

rm -rf /tmp/app
rm -rf /tmp/datascientist
mkdir -p /tmp/app/dataplatform/scripts/algorithm


mkdir -p /tmp/datascientist
mkdir -p /tmp/app/dataplatform/lib
mkdir -p /tmp/app/playmaker/evmodel

cp conf/env/dev/dataplatform.properties /tmp/app/dataplatform/
cp conf/env/dev/hadoop-metrics2.properties /tmp/app/dataplatform/
cp target/le-dataplatform*shaded.jar /tmp/app/dataplatform/lib
cp target/leframework.tar.gz /tmp/app/dataplatform/scripts
cp target/evpipeline.tar.gz /tmp/app/playmaker/evmodel
cp target/lepipeline.tar.gz /tmp/app/dataplatform/scripts
cp src/main/python/launcher.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipelinefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/rulefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipeline/pipeline.py /tmp/app/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pmmlpipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/datarules/rulepipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/evpipeline/evpipeline.py /tmp/app/playmaker/evmodel
cp src/main/python/algorithm/*.py /tmp/app/dataplatform/scripts/algorithm
cp src/test/resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py /tmp/datascientist



$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/app /
$HADOOP_HOME/bin/hadoop fs -copyFromLocal /tmp/datascientist /

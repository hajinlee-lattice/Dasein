#!/bin/bash

version=$(cat ${WSHOME}/le-parent/pom.xml | grep \<version\> | head -n 1 | cut -d \< -f 2 | cut -d \> -f 2)

rm -rf /tmp/app
mkdir -p /tmp/app/dataplatform/scripts/algorithm
mkdir -p /tmp/app/dataplatform/scripts/ledatascience/modelquality
mkdir -p /tmp/app/dataplatform/config/datascience

cp target/leframework.tar.gz /tmp/app/dataplatform/scripts
cp target/ledatascience.tar.gz /tmp/app/dataplatform/scripts
cp target/lepipeline.tar.gz /tmp/app/dataplatform/scripts
cp src/main/python/launcher.py /tmp/app/dataplatform/scripts
cp src/main/python/pipelinefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/rulefwk.py /tmp/app/dataplatform/scripts 
cp src/main/python/pipeline/pipeline.py /tmp/app/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pmmlpipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/datarules/rulepipeline.json /tmp/app/dataplatform/scripts
cp src/main/python/algorithm/*.py /tmp/app/dataplatform/scripts/algorithm
cp src/main/python/ledatascience/*.py /tmp/app/dataplatform/scripts/ledatascience
cp src/main/python/ledatascience/modelquality/*.py /tmp/app/dataplatform/scripts/ledatascience/modelquality
cp src/main/scripts/pythonlauncher.sh /tmp/app/dataplatform/scripts
cp ../le-serviceflows/cdl/src/test/python/apsdataloader.py /tmp/app/dataplatform/scripts
cp ../le-serviceflows/cdl/src/test/python/apsgenerator.py /tmp/app/dataplatform/scripts
cp ../le-docker/src/main/scripts/pythonapp/app.py /tmp/app/dataplatform/scripts
cp ../le-docker/src/main/scripts/pythonapp/apploader.py /tmp/app/dataplatform/scripts
cp src/main/python/leframework/webhdfs.py /tmp/app/dataplatform/scripts

cp src/main/python/ledatascience/modelquality/modelconfigs/*.csv /tmp/app/dataplatform/config/datascience

mkdir -p /tmp/app/playmaker/evmodel
cp target/evpipeline.tar.gz /tmp/app/playmaker/evmodel
cp src/main/python/evpipeline/evpipeline.py /tmp/app/playmaker/evmodel

hdfs dfs -mkdir -p /app/${LE_STACK}/${version}/dataplatform
hdfs dfs -rm -f -r /app/${LE_STACK}/${version}/dataplatform/scripts || true
hdfs dfs -copyFromLocal /tmp/app/dataplatform/scripts /app/${LE_STACK}/${version}/dataplatform
hdfs dfs -rm -f -r /app/${LE_STACK}/${version}/dataplatform/config || true
hdfs dfs -copyFromLocal /tmp/app/dataplatform/config /app/${LE_STACK}/${version}/dataplatform
hdfs dfs -put -f conf/env/dev/dataplatform.properties /app/${LE_STACK}/${version}/dataplatform
hdfs dfs -put -f conf/env/dev/hadoop-metrics2.properties /app/${LE_STACK}/${version}/dataplatform

hdfs dfs -mkdir -p /app/${LE_STACK}/${version}/playmaker
hdfs dfs -rm -f -r /app/${LE_STACK}/${version}/playmaker/evmodel || true
hdfs dfs -copyFromLocal /tmp/app/playmaker/evmodel /app/${LE_STACK}/${version}/playmaker/evmodel

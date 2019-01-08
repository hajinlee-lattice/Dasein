#!/usr/bin/env bash

dist="target/dist_python"
rm -rf ${dist} || true
mkdir ${dist}

mkdir -p ${dist}/dataplatform/scripts
cp target/leframework.tar.gz ${dist}/dataplatform/scripts
cp target/lepipeline.tar.gz ${dist}/dataplatform/scripts
cp src/main/python/launcher.py ${dist}/dataplatform/scripts
cp src/main/python/pipelinefwk.py ${dist}/dataplatform/scripts
cp src/main/python/rulefwk.py ${dist}/dataplatform/scripts
cp src/main/python/pipeline/pipeline.py ${dist}/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pipeline.json ${dist}/dataplatform/scripts
cp src/main/python/configurablepipelinetransformsfromfile/pmmlpipeline.json ${dist}/dataplatform/scripts
cp src/main/python/datarules/rulepipeline.json ${dist}/dataplatform/scripts
cp src/main/scripts/pythonlauncher.sh ${dist}/dataplatform/scripts
cp ../le-serviceflows/cdl/src/test/python/apsdataloader.py ${dist}/dataplatform/scripts
cp ../le-serviceflows/cdl/src/test/python/apsgenerator.py ${dist}/dataplatform/scripts
cp -r src/main/python/algorithm ${dist}/dataplatform/scripts/algorithm
cp ../le-docker/src/main/scripts/pythonapp/app.py ${dist}/dataplatform/scripts
cp ../le-docker/src/main/scripts/pythonapp/apploader.py ${dist}/dataplatform/scripts
cp src/main/python/leframework/webhdfs.py ${dist}/dataplatform/scripts

mkdir -p ${dist}/playmaker/evmodel
cp target/evpipeline.tar.gz ${dist}/playmaker/evmodel
cp src/main/python/evpipeline/evpipeline.py ${dist}/playmaker/evmodel

mkdir -p ${dist}/scoring/scripts
cp ../le-scoring/src/main/python/scoring.py ${dist}/scoring/scripts

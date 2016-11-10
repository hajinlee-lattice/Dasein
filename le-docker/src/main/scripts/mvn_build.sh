#!/bin/bash

pushd common; bash mvn_build.sh; popd
pushd tomcat; bash mvn_build.sh; popd
pushd nodejs; bash mvn_build.sh; popd
pushd zookeeper; bash mvn_build.sh; popd

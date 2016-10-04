#!/bin/bash

pushd tomcat; bash mvn_build.sh; popd
pushd zookeeper; bash mvn_build.sh; popd

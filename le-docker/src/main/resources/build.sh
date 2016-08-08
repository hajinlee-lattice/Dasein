#!/bin/bash

pushd common; bash build.sh; popd

pushd tomcat; bash build.sh; popd

pushd zookeeper; bash build.sh; popd

pushd kafka; bash build.sh; popd

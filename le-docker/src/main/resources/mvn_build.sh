#!/bin/bash

pushd tomcat; bash mvn_build.sh; popd
pushd nodejs; bash mvn_build.sh; popd
pushd httpd; bash mvn_build.sh; popd
pushd zookeeper; bash mvn_build.sh; popd
pushd haproxy; bash mvn_build.sh; popd

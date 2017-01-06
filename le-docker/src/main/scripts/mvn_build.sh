#!/bin/bash

pushd tomcat; bash mvn_build.sh; popd
pushd nodejs; bash mvn_build.sh; popd

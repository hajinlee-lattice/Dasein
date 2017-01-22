#!/bin/bash

pushd tomcat; bash build.sh; popd
pushd nodejs; bash build.sh; popd

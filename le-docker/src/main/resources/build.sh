#!/bin/bash

pushd zookeeper; bash build.sh; popd

pushd kafka; bash build.sh; popd

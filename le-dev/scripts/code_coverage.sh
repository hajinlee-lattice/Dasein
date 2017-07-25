#!/usr/bin/env bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

pushd le-coverage
mvn -Pmerge verify
pod

pushd le-coverage
mvn -Preport verify
pod
#!/usr/bin/env bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

. $WSHOME/le-dev/scripts/setuphdfs_datacloud.sh
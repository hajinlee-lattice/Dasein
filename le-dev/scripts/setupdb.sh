#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

sed "s|WSHOME|$WSHOME|g" setupdb.sql | mysql -u root -pwelcome

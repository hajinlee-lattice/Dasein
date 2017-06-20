#!/bin/bash

cat "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_springbatchdb.sql | eval $MYSQL_COMMAND
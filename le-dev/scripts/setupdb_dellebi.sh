#!/bin/bash

echo "Setting up DellEBI"
source $WSHOME/le-dev/scripts/setupdb_parameters.sh
sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_dellebi.sql | eval $MYSQL_COMMAND




#!/bin/bash

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_oauth2.sql | mysql -u root -pwelcome

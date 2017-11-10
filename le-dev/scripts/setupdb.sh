#!/bin/bash

function processErrors
{
  if [ $? -ne 0 ]
  then
      echo "Error!"
      cat /tmp/errors.txt
      exit 1
  fi
}

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

UNAME=`uname`

. $WSHOME/le-dev/scripts/setupzk.sh
. $WSHOME/le-dev/scripts/setuphdfs.sh

# delete existing ddl files
printf "%s\n" "Removing ddl_*.sql files from WSHOME: ${WSHOME}"
rm $WSHOME/ddl_*.sql

# Compile
mvn -T6 -f $WSHOME/db-pom.xml -DskipTests clean install

source $WSHOME/le-dev/scripts/setupdb_parameters.sh

. $WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh
. $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh
. $WSHOME/le-dev/scripts/setupdb_datadb.sh
. $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh
. $WSHOME/le-dev/scripts/setupdb_scoringdb.sh
. $WSHOME/le-dev/scripts/setupdb_oauth2.sh
. $WSHOME/le-dev/scripts/setupdb_globalauth.sh
. $WSHOME/le-dev/scripts/setupdb_quartzdb.sh

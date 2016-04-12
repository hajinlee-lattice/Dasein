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

# Compile le-domain
cd $WSHOME/le-domain
mvn -DskipTests clean install 2> /tmp/errors.txt
processErrors

# Compile le-dataplatform
cd $WSHOME/le-dataplatform
mvn -DskipTests clean install 2> /tmp/errors.txt
processErrors

# Compile le-db
cd $WSHOME/le-db
mvn -DskipTests clean install 2> /tmp/errors.txt
processErrors

$WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh
$WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh
$WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh
$WSHOME/le-dev/scripts/setupdb_oauth2.sh
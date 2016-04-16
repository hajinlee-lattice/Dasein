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

# Compile schema-pom
cd $WSHOME
mvn -T4 -f schema-pom.xml -DskipTests clean install 2> /tmp/errors.txt
processErrors

# Compile le-db
cd $WSHOME/le-db
mvn -DskipTests clean install 2> /tmp/errors.txt
processErrors

bash $WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh
bash $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh
bash $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh
bash $WSHOME/le-dev/scripts/setupdb_scoringdb.sh
bash $WSHOME/le-dev/scripts/setupdb_oauth2.sh
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

for project in 'db' 'dataplatform' 'propdata' 'scoring'
do
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-$project &&
    mvn -DskipTests clean install 2> /tmp/errors.txt &&
    popd &&
    processErrors &
done
wait

bash $WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh
bash $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh
bash $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh
bash $WSHOME/le-dev/scripts/setupdb_scoringdb.sh
bash $WSHOME/le-dev/scripts/setupdb_oauth2.sh
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

# 2019-09 Currently this script requires the working directory to be $WSHOME . Ref.
# https://confluence.lattice-engines.com/display/ENG/How+to+verify+a+new+environment
cd $WSHOME

UNAME=`uname`

. $WSHOME/le-dev/scripts/setupzk.sh

# delete existing ddl files
printf "%s\n" "Removing ddl_*.sql files from WSHOME: ${WSHOME}"
rm ${WSHOME}/ddl_*.sql

# Compile
mvn -T6 -f $WSHOME/db-pom.xml -DskipTests clean install

source $WSHOME/le-dev/scripts/setupdb_parameters.sh

. $WSHOME/le-dev/scripts/setupdb_globalauth.sh
. $WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh
. $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh
. $WSHOME/le-dev/scripts/setupdb_datadb.sh
. $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh
. $WSHOME/le-dev/scripts/setupdb_scoringdb.sh
. $WSHOME/le-dev/scripts/setupdb_oauth2.sh
. $WSHOME/le-dev/scripts/setupdb_quartzdb.sh
. $WSHOME/le-dev/scripts/setupdb_documentdb.sh
. $WSHOME/le-dev/scripts/setupdb_ldc_collectiondb.sh
. $WSHOME/le-dev/scripts/setupdb_dellebi.sh

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases
runtest serviceapps/cdl -g registertable -t RegisterLocalTestBucketedAccountTableTestNG


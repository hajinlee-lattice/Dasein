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

# Remove alter table drop foreign key statements from the script
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql

# Replace WSHOME with the environment variables that's been set
sed -i "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql
sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb.sql | mysql -u root -pwelcome

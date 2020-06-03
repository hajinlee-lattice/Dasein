#!/usr/bin/env bash

# usage: ./dbdiff.sh <PASSWORD> <ENV> <DB>

PASSWORD=$1

ENV=$2
ENV=${ENV:=qa}

DB=$3
DB=${DB:=PLS_MultiTenant}

if [ -z "$PASSWORD" ]
  then
    echo "MySQL password is not supplied"
    exit 1
fi

if [ "${ENV}" = "prod" ]; then
    AURORA_URL="lpi-encrypted-cluster.cluster-c6q8lwiagbkt.us-east-1.rds.amazonaws.com"
elif [ "${ENV}" = "qa" ]; then
    AURORA_URL="lpi-encrypted-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com"
fi

echo 'regenerate ddl'
pushd $WSHOME &&
mvn -T6 -f db-pom.xml -DskipTests package &&
popd

echo "using mysqldiff to find db diff in ${DB} ..."
mysqldiff \
    --server1=LPI:${PASSWORD}@${AURORA_URL} \
    --server2=root:welcome@127.0.0.1 \
    --skip-table-options \
    --compact \
    --force \
    --difftype=sql \
    ${DB}:${DB} > ${ENV}_diff.sql

python ${WSHOME}/le-dev/scripts/combine_sql.py -b ${DB} -d ${ENV}_diff.sql -g ${WSHOME}/ddl_pls_multitenant_mysql5innodb.sql -o ${ENV}_upgrade.sql

echo "upgrade script is generated at ${PWD}/${ENV}_upgrade.sql and ${PWD}/${ENV}_upgrade.sql.2"
#!/usr/bin/env bash

ENV=$1
ENV=${ENV:=qa}

DB=$2
DB=${DB:=PLS_MultiTenant}

if [ "${ENV}" = "prod" ]; then
    AURORA_URL="lpi-encrypted-cluster.cluster-c6q8lwiagbkt.us-east-1.rds.amazonaws.com"
    PASSWORD="3e!P@i8v|1"
elif [ "${ENV}" = "qa" ]; then
    AURORA_URL="lpi-encrypted-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com"
    PASSWORD="Lattice123"
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
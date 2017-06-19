#!/usr/bin/env bash

ENV=$1
ENV=${ENV:=qa}

DB=$2
DB=${DB:=PLS_MultiTenant}

if [ "${ENV}" = "prod" ]; then
    AURORA_URL="lpi-cluster.cluster-c6q8lwiagbkt.us-east-1.rds.amazonaws.com"
    PASSWORD="@uu5r3Ds!n=W"
elif [ "${ENV}" = "qa" ]; then
    AURORA_URL="lpi-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com"
    PASSWORD="Lattice123"
fi

echo 'using mysqldiff to find db diff ...'
mysqldiff \
    --server1=root:${PASSWORD}@${AURORA_URL} \
    --server2=root:welcome@127.0.0.1 \
    --skip-table-options \
    --compact \
    --force \
    --difftype=sql \
    ${DB}:${DB} > ${ENV}_diff.sql

python combine_sql.py -d ${ENV}_diff.sql -g ${WSHOME}/ddl_pls_multitenant_mysql5innodb.sql -o ${ENV}_update.sql
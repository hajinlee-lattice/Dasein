#!/usr/bin/env bash

tenantId="CDL_QA_Lina_Marketing_M36_6"
testDataVersion="7"

microserviceurl="https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com"

if [[ -d upload ]]; then
    rm -rf upload/*
else
  mkdir upload
fi

wget --no-check-certificate \
      --header 'MagicAuthentication: Security through obscurity!' \
      --header 'Content-Type: application/json' \
      --header 'cache-control: no-cache' \
      "${microserviceurl}/cdl/customerspaces/${tenantId}/datacollection/attrrepo" \
      -O upload/attrrepo.json

pushd upload && gzip attrrepo.json && popd

mkdir -p upload/TableJsons
mkdir -p upload/Tables

for role in \
    'ConsolidatedAccount' \
    'AnalyticPurchaseState' \
    'BucketedAccount' \
    'AccountExport' \
    'SortedContact' \
    'SortedProduct' \
    'SortedProductHierarchy' \
    'CalculatedCuratedAccountAttribute' \
    'CalculatedPurchaseHistory' \
    'CalculatedDepivotedPurchaseHistory' \
    'AggregatedTransaction' \
    'AggregatedPeriodTransaction' \
    'WebVisitProfile' \
    'OpportunityProfile' \
    'AccountMarketingActivityProfile' \
    'ContactMarketingActivityProfile' \
; do
    url="${microserviceurl}/cdl/customerspaces/${tenantId}/datacollection/tables?role=${role}"
    wget --no-check-certificate \
      --header 'MagicAuthentication: Security through obscurity!' \
      --header 'Content-Type: application/json' \
      --header 'cache-control: no-cache' \
      ${url} \
      -O upload/TableJsons/${role}.json
    if [[ -n `cat upload/TableJsons/${role}.json` ]]; then
      path=`cat upload/TableJsons/${role}.json | jq -r .extracts[0].path \
        | sed "s/\/Pods\/QA\/Contracts\/${tenantId}\/Tenants\/${tenantId}\/Spaces\/Production\///g" \
        | sed "s/\/\*\.avro//g" | sed "s/\/\*\.parquet//g"`
      table=`echo ${path} | cut -d / -f 3`
      echo "aws s3 sync s3://latticeengines-qa-customers/${tenantId}/atlas/${path} upload/Tables/${table}"
      aws s3 sync s3://latticeengines-qa-customers/${tenantId}/atlas/${path} upload/Tables/${table}
    else
      rm upload/TableJsons/${role}.json
    fi
done

pushd upload && zip Tables.zip -r Tables && zip TableJsons.zip -r TableJsons && popd

rm -rf upload/Tables
rm -rf upload/TableJsons

for role in \
    'BucketedAccount' \
    'SortedContact' \
    'SortedProduct' \
    'SortedProductHierarchy' \
    'CalculatedCuratedAccountAttribute' \
    'CalculatedDepivotedPurchaseHistory' \
    'AggregatedTransaction' \
    'AggregatedPeriodTransaction' \
    'WebVisitProfile' \
    'OpportunityProfile' \
    'AccountMarketingActivityProfile' \
    'ContactMarketingActivityProfile' \
; do

    url="${microserviceurl}/cdl/customerspaces/${tenantId}/datacollection/tablenames?role=${role}"
    tbl0=`curl -s -k \
      -H 'MagicAuthentication: Security through obscurity!' \
      -H 'Content-Type: application/json' \
      -H 'cache-control: no-cache' \
      ${url} \
    | jq -r .[0]`
    tbl1="Query_Test_${role}_${testDataVersion}"
    if [[ -n ${tbl0} ]] && [[ "${tbl0}" != "null" ]]; then
        echo "CREATE TABLE ${tbl1} (LIKE ${tbl0});"
        echo "INSERT INTO ${tbl1} (SELECT * FROM ${tbl0});"
    fi
done

#!/bin/bash

# Usage: bash copy-s3.sh slin_ga_test_05 SystemAccount_2020-03-10_06-55-55_UTC default
#
# Jupyter script will put the fixed table with name "<ORIGINAL_TABLE>-fix"
# this script back up the original table first by renaming with a suffix "-bk"
# then rename the fixed table to the original table name

BUCKET="latticeengines-qa-customers"
TENANT=$1
TABLE=$2
PROFILE=$3

S3_PATH="s3://$BUCKET/$TENANT/atlas/Data/Tables/$TABLE"

echo "COPY table $TABLE for tenant $TENANT"

BK=$S3_PATH"-bk"
FIX=$S3_PATH"-fix"

echo "Backup path $BK"
echo "Fix path $FIX"

aws s3 mv $S3_PATH $BK --recursive --profile $PROFILE
aws s3 mv $FIX $S3_PATH --recursive --profile $PROFILE

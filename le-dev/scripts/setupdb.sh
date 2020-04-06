#!/usr/bin/env bash

# colorize output
RED="$(tput setaf 1)"
YELLOW="$(tput setaf 3)"
OFF="$(tput sgr0)"

function sourceProcessErrors()
{
    local -r file="$1"
    shift
    source "$file" "$@"
    if [[ $? -ne 0 ]]
    then
	# ProcessErrors (actually, just notify user)
	echo "[${RED}ERROR${OFF}] Failure in $file"
    fi
}

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

# 2019-09 Currently this script requires the working directory to be $WSHOME . Ref.
# https://confluence.lattice-engines.com/display/ENG/How+to+verify+a+new+environment
cd "$WSHOME"

UNAME=`uname`

sourceProcessErrors "$WSHOME/le-dev/scripts/setupzk.sh"

# delete existing ddl files
printf "%s\n" "Removing ddl_*.sql files from WSHOME: $WSHOME"
rm -f "$WSHOME"/ddl_*.sql	# -f to avoid errors if no files

# Compile
mvn -T6 -f "$WSHOME/db-pom.xml" -DskipTests clean install

sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_parameters.sh"

sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_globalauth.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_pls_multitenant.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_ldc_managedb.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_datadb.sh"
# sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_leadscoringdb.sh"
# sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_scoringdb.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_oauth2.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_quartzdb.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_documentdb.sh"
sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_ldc_collectiondb.sh"
# sourceProcessErrors "$WSHOME/le-dev/scripts/setupdb_dellebi.sh"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source "$WSHOME/le-dev/aliases"
runtest serviceapps/cdl -g registertable -t RegisterLocalTestBucketedAccountTableTestNG


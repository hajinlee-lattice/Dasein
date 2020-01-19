#!/usr/bin/env bash

# Usage: ./spotbogs.sh <CHECKER?> <PROJECT?> -v
# >$ ./spotbogs.sh                       generate report for all projects
# >$ ./spotbogs.sh warning domain        generate report for le-domain using warning checker
# >$ ./spotbogs.sh warning domain -v     same as above but also print report to console

CHECKER=${1:-warning}
PROJECT=$2

echo "Generating spotbugs report using checker ${CHECKER}"

if [[ -n "$PROJECT" ]]; then
    pushd "./le-$PROJECT"
    mvn -q -Pspotbugs -Dchecker=${CHECKER} validate
else
    mvn -T4 -Pspotbugs -Dchecker=${CHECKER} validate
fi

if [[ -n "${PROJECT}" ]]; then
    popd
fi

if [[ -z "${PROJECT}" ]]; then
    PROJ_OPT=""
else
    PROJ_OPT="-p ${PROJECT}"
fi
# shift argument list by 2 to pass the remaining arguments to python script
# if project argument is not specified, an empty string will be passed to --proj flag
shift 2
echo "Generating aggregated report ..."
python $WSHOME/le-dev/scripts/spotbugs_report_generator.py ${PROJ_OPT} ${@:2}

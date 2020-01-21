#!/usr/bin/env bash

# Usage: ./spotbogs.sh <CHECKER?> <PROJECT?> -v
# >$ ./spotbogs.sh                       generate report for all projects
# >$ ./spotbogs.sh warning domain        generate report for le-domain using warning checker
# >$ ./spotbogs.sh warning domain -v     same as above but also print report to console

if [[ -z "$1" ]]; then
    CHECKER="warning"
else
    CHECKER="${1}"
    shift 1
fi

if [[ -z $@ ]] || [[ $@ == '-'* ]]; then
    echo "No project is specified, scan the whole repo"
    PROJECT=
else
    PROJECT=$1
    echo "Going to scan the specified project ${PROJECT}"
fi

echo "Generating spotbugs report using checker ${CHECKER}"
echo "### WARNING ###: Remember to build the project before scanning. SpotBugs works on bytecode."

if [[ -n "$PROJECT" ]]; then
    pushd "${WSHOME}/le-${PROJECT}"
    mvn -q -Pspotbugs -Dchecker=${CHECKER} validate
else
    mvn -T4 -q -Pspotbugs -Dchecker=${CHECKER} validate
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
if [[ -z "${PROJECT}" ]]; then
    ARGS="$@"
else
    ARGS="${@:2}"
fi
echo "Generating aggregated report ..."
python $WSHOME/le-dev/scripts/spotbugs_report_generator.py ${PROJ_OPT} ${ARGS}

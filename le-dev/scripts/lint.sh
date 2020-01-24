#!/usr/bin/env bash

# Usage: ./lint.sh <CHECKER?> <PROJECT?> -v
# >$ ./lint.sh                       generate report for all projects
# >$ ./lint.sh warning domain        generate report for le-domain using warning checker
# >$ ./lint.sh warning domain -v     same as above but also print report to console

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

echo "Generating lint report using checker ${CHECKER}"

if [[ -n "$PROJECT" ]]; then
    pushd "./le-$PROJECT"
fi

mvn -T8 -q -Pcheckstyle-report -Dconfig_loc=${WSHOME}/le-parent/checkstyle -Dchecker=${CHECKER} validate \
    && mvn -q -Pcheckstyle-report -Dconfig_loc=${WSHOME}/le-parent/checkstyle -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate \
    && mvn -q -Pcheckstyle-report -Dconfig_loc=${WSHOME}/le-parent/checkstyle -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate

if [[ -n "${PROJECT}" ]]; then
    popd
fi

if [[ -z "${PROJECT}" ]]; then
    PROJ_OPT=""
else
    PROJ_OPT="--proj ${PROJECT}"
fi
# shift argument list by 2 to pass the remaining arguments to python script
# if project argument is not specified, an empty string will be passed to --proj flag
if [[ -z "${PROJECT}" ]]; then
    ARGS="$@"
else
    ARGS="${@:2}"
fi
echo "Generating aggregated report ..."
python $WSHOME/le-dev/scripts/checkstyle_report_generator.py ${PROJ_OPT} ${ARGS}

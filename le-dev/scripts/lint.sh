#!/usr/bin/env bash

# Usage: ./lint.sh <CHECKER?> <PROJECT?> -v
# >$ ./lint.sh                       generate report for all projects
# >$ ./lint.sh warning domain        generate report for le-domain using warning checker
# >$ ./lint.sh warning domain -v     same as above but also print report to console

CHECKER=${1:-warning}
PROJECT=$2

echo "Generating lint report using checker ${CHECKER}"

if ! [ -z "$PROJECT" ]; then
    pushd "./le-$PROJECT"
fi

mvn -T8 -q -Pcheckstyle-report -Dchecker=${CHECKER} validate \
    && mvn -q -Pcheckstyle-report -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate \
    && mvn -q -Pcheckstyle-report -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate

if ! [ -z "$PROJECT" ]; then
    popd
fi

# shift argument list by 2 to pass the remaining arguments to python script
# if project argument is not specified, an empty string will be passed to --proj flag
shift 2
python $WSHOME/le-dev/scripts/checkstyle_report_generator.py --proj "$PROJECT" "$@"

#!/usr/bin/env bash

CHECKER=$1
CHECKER=${CHECKER:=warning}

echo "Generating lint report using checker ${CHECKER}"

mvn -T8 -Pcheckstyle-report -Dchecker=${CHECKER} validate \
    && mvn -Pcheckstyle-report -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate \
    && mvn -Pcheckstyle-report -Dchecker=${CHECKER} checkstyle:checkstyle-aggregate

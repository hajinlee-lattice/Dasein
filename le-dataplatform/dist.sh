#!/usr/bin/env bash

dist="target/dist"

rm -rf ${dist} || true
mkdir ${dist}

cp -r ../le-swlib/target/swlib ${dist}
for lib in 'leadprioritization' 'cdl' 'datacloud' 'modeling' 'scoring'; do
    if [ -d "${dist}/swlib/dataflowapi/le-serviceflows-${lib}" ]; then
        cp ../le-serviceflows/${lib}/target/le-*-shaded.jar ${dist}/swlib/dataflowapi/le-serviceflows-${lib}/le-serviceflows-${lib}.jar
    fi
    if [ -d "${dist}/swlib/workflowapi/le-serviceflows-${lib}" ]; then
        cp ../le-serviceflows/${lib}/target/le-*-shaded.jar ${dist}/swlib/workflowapi/le-serviceflows-${lib}/le-serviceflows-${lib}.jar
    fi
done

for lib in "dataflowapi" "eai" "workflowapi" "sqoop" "dataplatform" "dellebi" "scoring" "dataflow" "spark"; do
    mkdir -p ${dist}/${lib}/lib
    cp ../le-${lib}/target/le-*-shaded.jar ${dist}/${lib}/lib
done

mkdir -p ${dist}/datacloud/lib
cp ../le-datacloud/yarn/target/le-datacloud-yarn-*-shaded.jar ${dist}/datacloud/lib

echo "getting environment information"
echo ${LE_ENVIRONMENT}
echo ${LE_STACK}
echo ${STACK_PROFILE}
echo ${IS_MINISTACK}

mkdir -p ${dist}/lib
cp jacocoagent.jar ${dist}/lib

mkdir -p ${dist}/conf
cp ../le-config/conf/env/${LE_ENVIRONMENT}/latticeengines.properties ${dist}/conf
if [ ! -z "${STACK_PROFILE}" ]; then
    if [ "${IS_MINISTACK}" == "true" ]; then
        CONSUL_SERVER="internal-consul-1214146536.us-east-1.elb.amazonaws.com:8500"
        LE_STACK=${STACK}
        cp ../le-config/src/main/python/update_ministack.py .
        python2.7 update_ministack.py -e ${LE_ENVIRONMENT} -s ${LE_STACK} -c ${CONSUL_SERVER} -p ${STACK_PROFILE}
    fi
    cp ../le-config/src/main/python/replace_token.py .
    sed -i "s/LE_STACK=.*/LE_STACK=${LE_STACK}/" ${STACK_PROFILE}
    python2.7 replace_token.py ${dist}/conf ${STACK_PROFILE}
fi
cp ../le-config/conf/env/${LE_ENVIRONMENT}/log4j.properties ${dist}/conf
cp ../le-config/conf/env/${LE_ENVIRONMENT}/log4j2-yarn.xml ${dist}/conf

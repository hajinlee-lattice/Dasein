# using artifacts build on local to deploy remote workstation (a ministack)

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source ${WSHOME}/le-dev/aliases

STACK_NAME=${STACK_NAME:=${LE_STACK}}
echo "Assuming stack name is ${STACK_NAME}"
DOCKER_TAG=${DOCKER_TAG:=${STACK_NAME}}
echo "Assuming docker tag is ${DOCKER_TAG}"

STACK_IP=$(aws ec2 describe-instances --filter "Name=tag:Name,Values=${STACK_NAME}" Name=instance-state-name,Values=running --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)
echo "Found the ip or ministack ${STACK_NAME} is [${STACK_IP}]"

if [[ ${STACK_IP} == "None" ]] || [[ ${STACK_IP} == "" ]]; then
    echo "Stack IP [${STACK_IP}] is invalid, check your ministack provision!"
    exit -1
fi

PROP_FILE=${WSHOME}/le-config/conf/env/devcluster/latticeengines.properties
if [[ $(uname) == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' 's/${LE_STACK}/'${STACK_NAME}'/g' ${PROP_FILE}
    sed -i '' 's/${LE_CLIENT_ADDRESS}/'${STACK_IP}'/g' ${PROP_FILE}
else
    echo "You are on ${UNAME}"
    sed -i 's/${LE_STACK}/'${STACK_NAME}'/g' ${PROP_FILE}
    sed -i 's/${LE_CLIENT_ADDRESS}/'${STACK_IP}'/g' ${PROP_FILE}
fi

dkpull tomcat-jdk-11
dkpull tomcat-jre-8

pushd ${WSHOME}/le-docker
mvn -DskipTests compile


APPS=$1
if [[ -z ${APPS} ]]; then
    APPS="admin,pls,playmaker,oauth2,scoringapi,saml,matchapi,ulysses,dataflowapi,eai,metadata,modeling,scoring,workflowapi,quartz,datacloudapi,objectapi,cdl,lp"
    echo "APPS is not specified, going to build images for all apps: ${APPS}"
fi

cd src/main/scripts/tomcat
bash build.sh ${APPS}

export PYTHONPATH=${WSHOME}/le-dev/scripts:${PYTHONPATH}
ANACONDA_MINISTACK_PATH=${ANACONDA_MINISTACK_PATH:="${ANACONDA_HOME}/envs/ministack"}
for app in $(echo ${APPS} | sed "s/,/ /g"); do
    echo "====================================================================================="
    echo "WARNING: Going to push ${app} image to ECR, this is going to be very slow from local!"
    echo "====================================================================================="
    ${ANACONDA_MINISTACK_PATH}/bin/python -m docker push -e qa -t ${DOCKER_TAG} ${app}
done

popd

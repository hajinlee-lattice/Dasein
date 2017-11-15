#!/usr/bin/env bash

if [ "${AWS_KEY}" = "" ]; then
    echo "Must specify AWS_KEY to be your AWS Access Key"
    exit 1
fi

if [ "${AWS_SECRET}" = "" ]; then
    echo "Must specify AWS_SECRET to be your AWS Secret Key"
    exit 1
fi

pip install awscli --upgrade --user

AWS_HOME="${HOME}/.aws"
if [ ! -d "${AWS_HOME}" ]; then
    mkdir -p ${AWS_HOME}
fi

if [ -f "${AWS_HOME}/config" ]; then
    rm -rf ${AWS_HOME}/config
fi

if [ -f "${AWS_HOME}/config-qa" ]; then
    rm -rf ${AWS_HOME}/config-qa
fi

echo '[default]' > ${AWS_HOME}/config-qa
echo "aws_access_key_id=${AWS_KEY}" >> ${AWS_HOME}/config-qa
echo "aws_secret_access_key=${AWS_SECRET}" >> ${AWS_HOME}/config-qa
echo "region=us-east-1" >> ${AWS_HOME}/config-qa
echo "output=json" >> ${AWS_HOME}/config-qa

ln -s ${AWS_HOME}/config-qa ${AWS_HOME}/config

aws --version

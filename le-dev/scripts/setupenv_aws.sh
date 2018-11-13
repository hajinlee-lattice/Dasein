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

if [ `which pip3` != "" ]; then
    pip3 install awscli --upgrade --user
fi

AWS_HOME="${HOME}/.aws"
if [ ! -f "${AWS_HOME}/config" ]; then
    mkdir -p ${AWS_HOME} || true
    echo '[profile default]' > ${AWS_HOME}/config
    echo "aws_access_key_id=${AWS_KEY}" >> ${AWS_HOME}/config
    echo "aws_secret_access_key=${AWS_SECRET}" >> ${AWS_HOME}/config
    echo "region=us-east-1" >> ${AWS_HOME}/config
    echo "output=json" >> ${AWS_HOME}/config
fi

aws --version

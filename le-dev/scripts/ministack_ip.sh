#!/usr/bin/env bash

STACK=$1

PROFILE=$2
PROFILE=${PROFILE:=default}

if [[ -z "${STACK}" ]]; then
    echo "usage: ministack-ip STACK"
    exit 0
fi

aws ec2 describe-instances \
    --filter "Name=tag:Name,Values=${STACK}" \
    --query "Reservations[0].Instances[0].PrivateIpAddress" \
    --profile ${PROFILE} \
    --output text

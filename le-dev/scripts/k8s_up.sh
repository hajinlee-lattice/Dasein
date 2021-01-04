#!/usr/bin/env bash

pushd ${WSHOME}/le-dev/k8s
{
  kubectl apply -f zk.yml
  kubectl apply -f mysql.yml
  kubectl apply -f redis.yml
} && {
  popd
}

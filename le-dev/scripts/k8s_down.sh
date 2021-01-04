#!/usr/bin/env bash

pushd ${WSHOME}/le-dev/k8s
{
  kubectl delete -f zk.yml
  kubectl delete -f mysql.yml
  kubectl delete -f redis.yml
} && {
  popd
}

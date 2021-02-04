#!/usr/bin/env bash

if [[ "${USE_K8S}" == "true" ]]; then
pushd ${WSHOME}/le-dev/k8s
  {
    kubectl delete -f zk.yml
    kubectl delete -f mysql.yml
    kubectl delete -f redis.yml
  } && {
    popd
  }
else
  echo "USE_K8S=${USE_K8S}, skip tearing down minikube resources"
fi


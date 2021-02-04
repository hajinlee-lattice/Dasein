#!/usr/bin/env bash

if [[ "${USE_K8S}" == "true" ]]; then
  pushd ${WSHOME}/le-dev/k8s
  {
    kubectl apply -f le-pv.yml
    kubectl apply -f zk.yml
    kubectl apply -f mysql.yml
    kubectl apply -f redis.yml
  } && {
    popd
  }
else
  echo "USE_K8S=${USE_K8S}, skip bringing up minikube resources"
fi

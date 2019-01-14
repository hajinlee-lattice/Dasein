#!/usr/bin/env bash

if [[ -z ${GITHUB_USER} ]]; then
    echo "You must set GITHUB_USER"
    exit -1
fi

FORK_BRANCH=$1
UPSTREAM_BRANCH=$2

FORK_BRANCH=${FORK_BRANCH:=master}
UPSTREAM_BRANCH=${UPSTREAM_BRANCH:=develop}

echo "hub pull-request -b LatticeEngines:${UPSTREAM_BRANCH} -h ${GITHUB_USER}:${FORK_BRANCH}"
hub pull-request -b LatticeEngines:${UPSTREAM_BRANCH} -h ${GITHUB_USER}:${FORK_BRANCH}

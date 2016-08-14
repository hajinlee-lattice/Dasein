#!/usr/bin/env bash

export PYTHONPATH=$WSHOME/le-awsenvironment/src/main/python:$PYTHONPATH

python -m latticeengines.ecr.docker push $@
#!/usr/bin/env bash

export PYTHONPATH=${WSHOME}/le-dev/scripts:${PYTHONPATH}

python -m docker push $@
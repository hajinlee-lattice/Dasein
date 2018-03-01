#!/bin/bash

source $ANACONDA_HOME/bin/activate $1
py.test "${@:2}"
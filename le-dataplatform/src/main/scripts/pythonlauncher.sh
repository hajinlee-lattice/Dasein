#!/bin/bash

source $ANACONDA_HOME/bin/activate $1
python2 "${@:2}"
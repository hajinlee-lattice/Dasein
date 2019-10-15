#!/bin/bash

export PYTHONPATH=../../main/python:../../main/python/pipeline:$PYTHONPATH
/usr/local/bin/python2.7 -m unittest discover -p $1



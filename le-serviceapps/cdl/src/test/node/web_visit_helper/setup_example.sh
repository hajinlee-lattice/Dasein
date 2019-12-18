#!/bin/bash

WORKING_DIR="./example_data"

# remove dir if exists
if [ -d "$WORKING_DIR" ]; then rm -Rf $WORKING_DIR; fi

tar -xzvf web_visit_helper_example.tar.gz

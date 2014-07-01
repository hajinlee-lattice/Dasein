#!/bin/bash
find /home/LATTICE/build/.m2/repository/com/latticeengines/ -name 'le-*-[0-9].[0-9].[0-9]-201*' -mmin +720 -exec rm -rf {} \;
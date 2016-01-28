#!/bin/bash
shopt -s expand_aliases
source $HOME/.bash_aliases
cd $HOME/workspace/ledp
mvn -Pgenerate -DskipTests clean install
cd le-dataplatform && dpdpl
cd ../le-eai && eaidpl
cd ../le-dataflow && dfdpl
cd ../le-dataflowapi && dfapidpl
cd ../le-workflowapi && wfapidpl
cd ../le-swlib && swlibdpl
cd ../le-microservice && microservicedpl

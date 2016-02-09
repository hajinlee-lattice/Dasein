#!/bin/bash

function runCommand()
{
  $1 2> /dev/null
  if [ $? -eq 0 ]
  then
      exit 0
  fi
}

shopt -s expand_aliases
source $HOME/.bash_aliases
cd $HOME/workspace/ledp
runCommand "mvn -Pgenerate -DskipTests clean install"
runCommand "cd le-dataplatform && dpdpl"
runCommand "cd ../le-eai && eaidpl"
runCommand "cd ../le-dataflow && dfdpl"
runCommand "cd ../le-dataflowapi && dfapidpl"
runCommand "cd ../le-workflowapi && wfapidpl"
runCommand "cd ../le-swlib && swlibdpl"
runCommand "cd ../le-microservice && microservicedpl"



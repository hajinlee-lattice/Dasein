#!/bin/bash

function runCommand()
{
  $1
  if [ $? -eq 0 ]
  then
      exit 0
  fi
}

echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $HOME/.bash_aliases
echo "Changing dir into workspace"
cd $HOME/workspace/ledp
echo "Top-level compile"
runCommand "mvn -Pgenerate -DskipTests clean install"
echo "Deploying dataplatform to local Hadoop"
runCommand "cd le-dataplatform && dpdpl"
echo "Deploying eai to local Hadoop"
runCommand "cd ../le-eai && eaidpl"
echo "Deploying dataflow to local Hadoop"
runCommand "cd ../le-dataflow && dfdpl"
echo "Deploying dataflowapi to local Hadoop"
runCommand "cd ../le-dataflowapi && dfapidpl"
echo "Deploying workflow to local Hadoop"
runCommand "cd ../le-workflow && wfdpl"
echo "Deploying workflowapi to local Hadoop"
runCommand "cd ../le-workflowapi && wfapidpl"
echo "Deploying swlib to local Hadoop"
runCommand "cd ../le-swlib && swlibdpl"
echo "Deploying microservices to local jetty"
runCommand "cd ../le-microservice && microservicedpl"



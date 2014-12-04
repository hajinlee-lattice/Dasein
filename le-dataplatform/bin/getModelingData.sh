#!/bin/bash

function usage {
	echo $'usage: ./getModelingData.sh <appId> <customer> <environment> \nenvironment should be one of the following: prod, integ, local'
	exit
}

# get arguments
appId=$1
customer=$2
environment=$3

if [ $# -ne 3 ]; then
usage
fi

# set environment
case "$environment" in
	"integ")
		host=10.41.1.211
		localEnv=false
		;;
	"prod")
		host=10.51.1.196
		localEnv=false
		;;
	"local")
		localEnv=true
		;;
	*)
		usage
		;;
esac

# gather data files from HDFS
commands="rm -rf /tmp/modelingData; mkdir /tmp/modelingData; hadoop fs -copyToLocal /app-logs/yarn/logs/$appId/* /user/s-analytics/customers/$customer/data/Q_*/samples/* /user/s-analytics/customers/$customer/data/Q_*/*.avsc /user/s-analytics/customers/$customer/data/EventMetadata/* /tmp/modelingData; cd /tmp/modelingData; tar -cvf $appId-$customer-data.tar ./"
case $localEnv in
	false)
		echo "running commands in $environment"
		ssh $host $commands
		# copy to local
		scp $host:/tmp/modelingData/$appId-$customer-data.tar  ./
		;;
	true)
		echo "running commands in local"
		eval $commands
		;;
esac


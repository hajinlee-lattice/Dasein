#!/bin/bash -x
IFS=', ' read -a array <<< ${HostName}

for host in "${array[@]}"
do
  scp -o StrictHostKeyChecking=no -p ../le-release/src/main/scripts/dependencies_install ${host}:/tmp
  #copy target pom file to the repository
  pushd target/rpm/le-dataplatform-hdfs/RPMS/noarch
  rpm=`basename le-dataplatform-hdfs*`
  echo "start ssh ${host}"
  scp -p $rpm ${host}:/tmp
  ssh -t ${host} "cd /tmp; chmod 744 $rpm dependencies_install; ./dependencies_install; rpm -e le-dataplatform-hdfs; rpm -i --replacefiles $rpm; rm -f $rpm dependencies_install"
  popd
done

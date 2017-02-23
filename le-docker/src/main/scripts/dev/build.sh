#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

build_docker() {
	IMAGE=$1
	sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
	docker build -t $IMAGE . || true
	mv Dockerfile.bak Dockerfile
}

#RUN wget -q http://10.41.1.10/tars/apache-tomcat-8.5.8.tar.gz -O /root/chefcache/apache-tomcat-8.5.8.tar.gz \
#    && tar xfz  /root/chefcache/apache-tomcat-8.5.8.tar.gz \
#    && rm -f /root/chefcache/apache-tomcat-8.5.8.tar.gz
#RUN wget  -q  http://10.41.1.10/tars/apache-tomcat-8.5.8-deployer.tar.gz -O  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
#    && tar xfz /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
#    && rm -f  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz
#RUN  wget -q http://10.41.1.10/tars/catalina-jmx-remote.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-jmx-remote.jar \
#     && wget -q http://10.41.1.10/tars/catalina-ws.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-ws.jar \
#    && chown -R tomcat.tomcat /opt/apache-tomcat-8.5.8

pushd le-tomcat-dev
build_docker latticeengines/tomcat-dev
popd

pushd le-dynamo
build_docker latticeengines/dynamo
popd

pushd le-mysql
build_docker latticeengines/mysql
popd

pushd le-zookeeper
build_docker latticeengines/zookeeper
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null
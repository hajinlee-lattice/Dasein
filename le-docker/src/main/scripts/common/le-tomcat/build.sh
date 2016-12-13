mkdir -p binaries

pushd binaries
if [ `basename $PWD`  != 'binaries' ] ; then
    echo $PWD
    echo "Not where we are supposed to be"
    exit -1
fi

if [ ! -f "apache-tomcat-8.5.8-deployer.tar.gz" ]; then
    wget http://10.41.1.10/tars/apache-tomcat-8.5.8-deployer.tar.gz
fi

if [ ! -f "apache-tomcat-8.5.8.tar.gz" ]; then
    wget http://10.41.1.10/tars/apache-tomcat-8.5.8.tar.gz
fi

if [ ! -f "catalina-jmx-remote.jar" ]; then
    wget http://10.41.1.10/tars/catalina-jmx-remote.jar
fi

if [ ! -f "catalina-ws.jar" ]; then
    wget http://10.41.1.10/tars/catalina-ws.jar
fi

if [ ! -f "jdk-8u101-linux-x64.rpm" ]; then
    wget http://10.41.1.10/tars/jdk-8u101-linux-x64.rpm
fi

popd

source ../../functions.sh
build_docker latticeengines/tomcat
docker tag latticeengines/tomcat:latest latticeengines/tomcat:8.5

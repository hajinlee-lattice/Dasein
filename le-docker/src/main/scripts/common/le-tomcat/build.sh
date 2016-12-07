mkdir -p binaries
rm -f binaries/*
pushd binaries
if [ `basename $PWD`  != 'binaries' ] ; then
    echo $PWD
    echo "Not where we are supposed to be"
    exit -1
fi
    
wget http://10.41.1.10/tars/apache-tomcat-8.5.8-deployer.tar.gz
wget http://10.41.1.10/tars/apache-tomcat-8.5.8.tar.gz
wget http://10.41.1.10/tars/catalina-jmx-remote.jar
wget http://10.41.1.10/tars/catalina-ws.jar
wget http://10.41.1.10/tars/jdk-8u101-linux-x64.rpm
popd
docker build -t latticeengines/tomcat .
docker build -t latticeengines/tomcat:8.5 .


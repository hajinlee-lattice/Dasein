
source ../../functions.sh
build_docker latticeengines/tomcat
docker tag -f latticeengines/tomcat:latest latticeengines/tomcat:8.5

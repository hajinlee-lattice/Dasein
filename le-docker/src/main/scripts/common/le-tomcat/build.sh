
source ../../functions.sh
build_docker latticeengines/tomcat
docker tag latticeengines/tomcat:latest latticeengines/tomcat:9.0.5
docker tag latticeengines/tomcat:latest latticeengines/tomcat:9.0
docker tag latticeengines/tomcat:latest quay.io/latticesoftware/tomcat:9.0

source ../../functions.sh
build_docker latticeengines/jdk
docker tag -f latticeengines/jdk:latest latticeengines/jdk:1.8

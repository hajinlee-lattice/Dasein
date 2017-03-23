source ../../functions.sh
build_docker latticeengines/jre
docker tag -f latticeengines/jre:latest latticeengines/jre:1.8

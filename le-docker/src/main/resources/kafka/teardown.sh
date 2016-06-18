if [ -z ${KAFKA_NODES} ]; then
    KAFKA_NODES=3
fi

KAFKA=$1
if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi

# cleanup
for i in $(seq 1 $KAFKA_NODES);
do 
    echo "stopping ${KAFKA}${i}"
    docker stop ${KAFKA}${i} 2> /dev/null || true
done

echo "stopping haproxy ${KAFKA}-haproxy"
docker stop ${KAFKA}-ha 2> /dev/null || true

docker rm $(docker ps -a -q) 2> /dev/null || true
docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

PROXY_DIR=${KAFKA}_proxy
rm -rf ${PROXY_DIR}

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"
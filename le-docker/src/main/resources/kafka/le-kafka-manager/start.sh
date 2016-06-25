#!/usr/bin/env bash

if [ "${ZK_HOSTS}" = "localhost:2181" ]; then

    /usr/zookeeper/bin/zkServer.sh stop || true
    /usr/zookeeper/bin/zkServer.sh start
fi

if [ -f "/kafka-manager-1.3.0.8/RUNNING_PID" ]; then
    kill -9 `cat /kafka-manager-1.3.0.8/RUNNING_PID`
    rm -rf /kafka-manager-1.3.0.8/RUNNING_PID
fi

bin/kafka-manager -Dconfig.file=conf/application.conf

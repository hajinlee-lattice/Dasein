#!/usr/bin/env bash

if [ "${ZK_HOSTS}" = "localhost:2181" ]; then

    /usr/zookeeper/bin/zkServer.sh stop || true
    /usr/zookeeper/bin/zkServer.sh start
    if [ -f "/etc/zk_clean_cron_running" ]; then
        crontab -l | { cat; echo "0 * * * * bash -c /usr/zookeeper/bin/zkCleanup.sh 5"; } | crontab -
        touch /etc/zk_clean_cron_running
    fi
fi

if [ -f "/kafka-manager-1.3.0.8/RUNNING_PID" ]; then
    kill -9 `cat /kafka-manager-1.3.0.8/RUNNING_PID`
    rm -rf /kafka-manager-1.3.0.8/RUNNING_PID
fi

bin/kafka-manager -Dconfig.file=conf/application.conf

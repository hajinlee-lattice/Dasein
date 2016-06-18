#!/usr/bin/env bash

/usr/zookeeper/bin/zkServer.sh start

bin/kafka-manager -Dconfig.file=conf/application.conf

#!/usr/bin/env bash
#
# ZooKeeper
#
# chkconfig: 2345 89 9
# description: zookeeper
ZOOKEEPER='/opt/zookeeper'

source /etc/rc.d/init.d/functions
source $ZOOKEEPER/bin/zkEnv.sh

RETVAL=0
PIDFILE="/var/lib/zookeeper/data/zookeeper_server.pid"
desc="ZooKeeper daemon"

start() {
  echo -n $"Starting $desc (zookeeper): "
  daemon $ZOOKEEPER/bin/zkServer.sh start
  RETVAL=$?
  echo
  [ $RETVAL -eq 0 ] && touch /var/lock/subsys/zookeeper
  return $RETVAL
}

stop() {
  echo -n $"Stopping $desc (zookeeper): "
  daemon $ZOOKEEPER/bin/zkServer.sh stop
  RETVAL=$?
  sleep 5
  echo
  [ $RETVAL -eq 0 ] && rm -f /var/lock/subsys/zookeeper $PIDFILE
}

restart() {
  stop
  start
}

get_pid() {
  cat "$PIDFILE"
}

checkstatus(){
  status -p $PIDFILE ${JAVA_HOME}/bin/java
  RETVAL=$?
}

condrestart(){
  [ -e /var/lock/subsys/zookeeper ] && restart || :
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    checkstatus
    ;;
  restart)
    restart
    ;;
  condrestart)
    condrestart
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|restart|condrestart}"
    exit 1
esac

exit $RETVAL
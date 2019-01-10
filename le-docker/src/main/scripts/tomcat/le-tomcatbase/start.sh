#!/usr/bin/env bash
mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat:tomcat ${CATALINA_HOME}

ulimit -n 4096

if [[ "${ENABLE_JACOCO}" == "true" ]]; then
    pid=0
    export CATALINA_PID=/var/run/tomcat

    # SIGTERM-handler
    term_handler() {
      if [[ $pid -ne 0 ]]; then
        echo 'in SIGTERM handler'
        JACOCO_DEST_FILE="/mnt/efs/jacoco/${HOSTNAME}.exec"
        if [[ -f "${JACOCO_DEST_FILE}" ]]; then
            chmod a+wr ${JACOCO_DEST_FILE}
        fi
        kill -SIGTERM "$pid"
        wait "$pid"
      fi
      exit 143; # 128 + 15 -- SIGTERM
    }

    trap 'kill ${!}; term_handler' SIGTERM
    ${CATALINA_HOME}/bin/catalina.sh run &
    pid="$!"
    echo "pid=${pid}"

    # wait forever
    while true
    do
      tail -f ${CATALINA_HOME}/logs/catalina*.log & wait ${!}
    done
else
    ${CATALINA_HOME}/bin/catalina.sh run
fi



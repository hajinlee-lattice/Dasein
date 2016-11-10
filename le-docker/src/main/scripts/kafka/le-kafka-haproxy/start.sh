#!/usr/bin/env bash

SR_HOSTS=""
KR_HOSTS=""
KC_HOSTS=""
for HOST in $HOSTS;
do
	while [ 1 == 1 ]; do
		SERVER_IP=$(dig +short $HOST)
		if [ -z SERVER_IP ]; then
			continue
		else
			break
		fi
	done

    if [[ $HOST == *"sr"* ]]
    then
        SR_HOSTS="${SR_HOSTS}\n    server ${HOST} ${SERVER_IP}:9022 check"
    fi

    if [[ $HOST == *"rest"* ]]
    then
        KR_HOSTS="${KR_HOSTS}\n    server ${HOST} ${SERVER_IP}:9023 check"
    fi

    if [[ $HOST == *"conn"* ]]
    then
        KC_HOSTS="${KC_HOSTS}\n    server ${HOST} ${SERVER_IP}:9024 check"
    fi
done

sed -i "s/#{{SR_HOSTS}}/${SR_HOSTS}/" /usr/local/etc/haproxy/haproxy.cfg
sed -i "s/#{{KR_HOSTS}}/${KR_HOSTS}/" /usr/local/etc/haproxy/haproxy.cfg
sed -i "s/#{{KC_HOSTS}}/${KC_HOSTS}/" /usr/local/etc/haproxy/haproxy.cfg

/docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg

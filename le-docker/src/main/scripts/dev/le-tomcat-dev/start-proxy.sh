#!/usr/bin/env bash

cat /usr/local/etc/haproxy/haproxy.cfg
nohup /docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg &
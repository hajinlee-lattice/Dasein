#!/usr/bin/env bash

cat /etc/haproxy.conf
nohup haproxy -f /etc/haproxy.conf & 2&>1 > /var/log/haproxy
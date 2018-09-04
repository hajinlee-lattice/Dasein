#!/usr/bin/env bash -xv

ls -la /etc/ledp

if [ -f "/etc/ledp/lattice.crt" ]; then
    cp -f /etc/ledp/lattice.crt /etc/pki/tls/server.crt
fi

if [ -f "/etc/ledp/lattice.key" ]; then
    cp -f /etc/ledp/lattice.key /etc/pki/tls/private/server.key
fi

python /index.py ${SWAGGER_APPS} 

cat /var/www/html/index.html

/usr/sbin/httpd -DNO_DETACH

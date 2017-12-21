#!/usr/bin/env bash

if [ -f "/etc/ledp/lattice.crt" ]; then
    cp -f /etc/ledp/lattice.crt /usr/local/apache2/conf/server.crt
fi

if [ -f "/etc/ledp/lattice.key" ]; then
    cp -f /etc/ledp/lattice.key /usr/local/apache2/conf/server.key
fi

python /index.py ${SWAGGER_APPS}

cat /usr/local/apache2/htdocs/index.html

/usr/local/bin/httpd-foreground
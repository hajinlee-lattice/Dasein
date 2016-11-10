#!/usr/bin/env bash

python /index.py ${SWAGGER_APPS}

cat /usr/local/apache2/htdocs/index.html

/usr/local/bin/httpd-foreground
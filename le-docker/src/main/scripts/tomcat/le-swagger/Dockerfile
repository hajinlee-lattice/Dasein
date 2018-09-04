FROM latticeengines/centos7
RUN yum install -y httpd mod_ssl && yum clean all

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY httpd.conf /etc/httpd/conf/httpd.conf
RUN rm -f /etc/httpd/conf.d/userdor.conf /etc/httpd/conf.d/welcome.conf
COPY httpd-ssl.conf /etc/httpd/conf.d/ssl.conf
COPY index.py /index.py
COPY ./web/ /var/www/html
COPY start.sh /start.sh

CMD ["bash", "/start.sh"]

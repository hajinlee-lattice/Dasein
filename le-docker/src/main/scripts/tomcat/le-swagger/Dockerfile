FROM httpd:2.4

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		python \
	&& rm -r /var/lib/apt/lists/*
RUN rm /usr/local/apache2/htdocs/index.html

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY httpd.conf /usr/local/apache2/conf/httpd.conf
COPY httpd-ssl.conf /usr/local/apache2/conf/extra/httpd-ssl.conf
COPY index.py /index.py
COPY ./web/ /usr/local/apache2/htdocs/
COPY start.sh /start.sh

CMD ["bash", "/start.sh"]
FROM latticeengines/httpd

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		python \
	&& rm -r /var/lib/apt/lists/*
RUN rm /usr/local/apache2/htdocs/index.html

# put a random value here to invalidate docker cache
RUN echo invalidate cache 1478637311 > /dev/null
COPY index.py /index.py
COPY ./web/ /usr/local/apache2/htdocs/
COPY start.sh /start.sh

CMD ["bash", "/start.sh"]
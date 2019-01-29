FROM latticeengines/nodejs

RUN npm install express -g
ENV APP_ROOT /opt/node/le-ui
WORKDIR ${APP_ROOT}
EXPOSE 3000 3002

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD le-ui.tar.gz /opt/node

COPY start.sh /start.sh

CMD ["bash", "/start.sh"]

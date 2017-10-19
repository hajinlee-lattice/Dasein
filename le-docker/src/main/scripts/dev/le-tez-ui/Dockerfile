FROM latticeengines/tomcat

RUN rm -rf $CATALINA_HOME/webapps/*

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD tez-ui.tar.gz $CATALINA_HOME/webapps/tez-ui
ADD start.sh /start.sh

CMD [ "/bin/bash", "/start.sh" ]

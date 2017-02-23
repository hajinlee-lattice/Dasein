FROM latticeengines/centos7

ENV DYNAMO_HOME /opt/dynamo

ADD dynamodb_local_latest.tar.gz $DYNAMO_HOME
#RUN wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz \
#    && mkdir -p $DYNAMO_HOME \
#    && cd $DYNAMO_HOME \
#	&& tar xzf /dynamodb_local_latest.tar.gz \
#	&& rm /dynamodb_local_latest.tar.gz

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

ADD start.sh /start.sh
EXPOSE 8000

CMD ["bash", "/start.sh"]
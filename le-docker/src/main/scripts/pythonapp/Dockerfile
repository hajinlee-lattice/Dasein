FROM  latticeengines/python

WORKDIR /
COPY apsdataloader.py /
COPY apsgenerator.py /
COPY start.sh /
COPY leframework.tar.gz /
RUN tar -xvzf leframework.tar.gz

ENV PYTHON_PATH /opt/conda/bin:/:/leframework:$PYTHON_PATH

ENTRYPOINT ["/start.sh"]

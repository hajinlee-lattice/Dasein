FROM  latticeengines/python

WORKDIR /
COPY apsdataloader.py /
COPY apsgenerator.py /

COPY app.py /
COPY apploader.py /

COPY start.sh /
COPY webhdfs.py /

ENV PYTHON_PATH /opt/conda/bin:/

ENTRYPOINT ["/start.sh"]

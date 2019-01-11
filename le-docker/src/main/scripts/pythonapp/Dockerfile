FROM  latticeengines/python

WORKDIR /python_app
COPY apsdataloader.py /python_app
COPY apsgenerator.py /python_app

COPY app.py /python_app
COPY apploader.py /python_app

COPY start.sh /python_app
#COPY start_dev_aps.sh /python_app
#COPY start_dev_aws.sh /python_app

COPY webhdfs.py /python_app

COPY libgcrypt.so.11.8.2 /python_app

ENV PYTHON_PATH /opt/conda/bin:/

ENTRYPOINT ["/python_app/start.sh"]

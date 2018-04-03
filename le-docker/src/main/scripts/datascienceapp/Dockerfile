FROM  latticeengines/python

RUN conda create --name ledatascience --clone lattice
RUN source activate ledatascience

RUN source activate ledatascience && pip install openpyxl==2.5.0
RUN source deactivate

WORKDIR /

COPY start.sh /
COPY ledatascience.tar.gz /
COPY conf/latticeengines.properties /
RUN tar -xvzf ledatascience.tar.gz

ENV PYTHON_PATH /opt/conda/bin:/:/ledatascience/modelquality:$PYTHON_PATH

ENTRYPOINT ["/start.sh"]

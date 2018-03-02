FROM latticeengines/centos7

RUN yum -y install wget which  bzip2  && yum clean all 
WORKDIR /

# Install anaconda
RUN wget https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && /bin/bash Anaconda2-4.3.0-Linux-x86_64.sh -b -p /opt/conda \
    && rm Anaconda2-4.3.0-Linux-x86_64.sh

#  && ln -s /opt/conda/bin/python /usr/local/bin/python2.7 \

#ENV PATH /opt/conda/bin:/:$PATH
#ENV PYTHON_PATH /opt/conda/bin:/:$PYTHON_PATH
# remove / 
ENV PATH /opt/conda/bin:$PATH
ENV PYTHON_PATH /opt/conda/bin:$PYTHON_PATH

RUN conda create -y --name lattice_20180301 python=2.7.13 pip
RUN source activate lattice_20180301

RUN source activate lattice_20180301 && pip install  avro==1.7.7 fastavro==0.7.7 kazoo==2.2.1 patsy==0.3.0 pexpect==4.0.1 ptyprocess==0.5.1 python-dateutil==2.4.1
#RUN source activate lattice_20180301 && conda install -y psutil=2.2.1
RUN source activate lattice_20180301 && conda install -y pandas=0.13.1=np18py27_0
RUN source activate lattice_20180301 && conda install -y statsmodels==0.5.0  numpy=1.9.3=py27_2  pandas=0.17.1=np19py27_0  scipy=0.17.0=np19py27_1
# statsmodel install updates:
# numpy:           1.8.2-py27_1      --> 1.9.3-py27_2     
# pandas:          0.13.1-np18py27_0 --> 0.17.1-np19py27_0
# scipy:           0.14.0-np18py27_0 --> 0.17.0-np19py27_1

RUN source activate lattice_20180301 && conda install -y psutil=2.2.1 libiconv=1.14=0 libxml2=2.9.4=0 libxslt=1.1.28=3 lxml=3.4.0=py27_0 openssl=1.0.2j=0 py=1.4.31=py27_0 pytest=2.9.2=py27_0 pytz=2016.10=py27_0 readline=6.2=2 scikit-learn=0.14.1=np18py27_1 setuptools=27.2.0=py27_0 sqlite=3.13.0=0 tk=8.5.18=0 wheel=0.29.0=py27_0 zlib=1.2.8=3  
# The following packages will be DOWNGRADED due to dependency conflicts:
# 
#     numpy:        1.9.3-py27_2      --> 1.8.2-py27_1     
#     openssl:      1.0.2l-0          --> 1.0.2j-0         
#     pandas:       0.17.1-np19py27_0 --> 0.14.1-np18py27_0
#     pytz:         2017.2-py27_0     --> 2016.10-py27_0   
#     scipy:        0.17.0-np19py27_1 --> 0.14.0-np18py27_0
#     setuptools:   36.4.0-py27_1     --> 27.2.0-py27_0    
#     statsmodels:  0.5.0-np19py27_2  --> 0.5.0-np18py27_0 
#     zlib:         1.2.11-0          --> 1.2.8-3          

# Done for backward compatibility
RUN conda create --name lattice  --clone lattice_20180301

RUN source deactivate

RUN conda create -y --name v01_20180301 python=2.7.13 pip
RUN source activate v01_20180301

RUN source activate v01_20180301 && pip install avro==1.8.1 fastavro==0.12.1 pexpect==4.2.1 ptyprocess==0.5.1 kazoo==2.2.1 patsy==0.4.1
RUN source activate v01_20180301 && conda install -y psutil=5.2.0 pandas=0.19.2=np112py27_1
RUN source activate v01_20180301 && pip install statsmodels==0.8.0
RUN source activate v01_20180301 && conda install -y python-snappy==0.5.1

RUN source activate v01_20180301 && conda install -y libiconv=1.14=0 libxml2=2.9.4=0 libxslt=1.1.29=0 lxml=3.7.3=py27_0 numpy=1.12.0=py27_0 openssl=1.0.2k=1 py=1.4.32=py27_0 \
    pytest=3.0.6=py27_0 pytz=2016.10=py27_0 readline=6.2=2 scikit-learn=0.18.1=np112py27_1 setuptools=27.2.0=py27_0 sqlite=3.13.0=0 \
    tk=8.5.18=0 wheel=0.29.0=py27_0 zlib=1.2.8=3

# The following packages will be DOWNGRADED due to dependency conflicts:
# 
#     numpy:        1.12.1-py27_0      --> 1.12.0-py27_0 
#     openssl:      1.0.2l-0           --> 1.0.2k-1      
#     pytz:         2017.2-py27_0      --> 2016.10-py27_0
#     setuptools:   36.4.0-py27_1      --> 27.2.0-py27_0 
#     zlib:         1.2.11-0           --> 1.2.8-3       
RUN source activate v01_20180301 && pip install sklearn-pandas==1.3.0
#RUN source activate v01_20180301 && pip install git+https://github.com/jpmml/sklearn2pmml.git@0.17.4
RUN source deactivate

# Done for backward compatibility
RUN conda create --name v01  --clone v01_20180301

FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y \
    software-properties-common \
    python-software-properties \
    net-tools \
    curl \
    wget \
    openssh-server \
    bzip2 \
    vim

# Install Java.
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle


# Install miniconda
RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh \
    && /bin/bash Miniconda2-latest-Linux-x86_64.sh -b -p /opt/conda \
    && ln -s /opt/conda/bin/python /usr/local/bin/python2.7 \
    && ln -s /opt/conda/bin/coverage /usr/local/bin/coverage \
    && rm Miniconda2-latest-Linux-x86_64.sh
ENV PATH /opt/conda/bin:$PATH
ENV PYTHON_PATH /opt/conda/bin/python

# password less
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN systemctl enable ssh

ENTRYPOINT ["bash", "--login"]
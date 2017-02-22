#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"
printf "%s\n" "${HADOOP_NAMENODE_DATA_DIR:?You must set HADOOP_NAMENODE_DATA_DIR}"
printf "%s\n" "${HADOOP_DATANODE_DATA_DIR:?You must set HADOOP_DATANODE_DATA_DIR}"
printf "%s\n" "${DYNAMO_HOME:?You must set DYNAMO_HOME}"
printf "%s\n" "${ANACONDA_HOME:?You must set ANACONDA_HOME}"

bash $WSHOME/le-dev/start-cluster.sh || true

cp $WSHOME/le-dev/hadoop/dev/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/yarn-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/kms-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/core-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/hdfs-site.xml $HADOOP_CONF_DIR
sed -i".orig" "s|[$][{]HADOOP_NAMENODE_DATA_DIR[}]|${HADOOP_NAMENODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i".orig" "s|[$][{]HADOOP_DATANODE_DATA_DIR[}]|${HADOOP_DATANODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml

if [ ! -z "CATALINA_HOME" ]; then
    cp $WSHOME/le-dev/tomcat/dev/server.xml $CATALINA_HOME/conf/server.xml
fi

rm -rf $WSHOME/le-dev/testartifacts/certificates/cacerts || true
wget -P $WSHOME/le-dev/testartifacts/certificates http://10.41.1.10/tars/cacerts
sudo mkdir -p /etc/ledp
sudo cp $WSHOME/le-dev/testartifacts/certificates/cacerts /etc/ledp

sudo pip install -r $WSHOME/le-dev/scripts/requirements.txt || true
pip install -r $WSHOME/le-dev/scripts/requirements.txt || true

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi

existing=$(hadoop key list | grep master)
if [ -z "$existing" ]; then
    hadoop key create master
else
    echo "Master key already installed"
fi

mkdir -p $WSHOME/le-dev/hadoop/artifacts/
export TEZ_TARBALL=$WSHOME/le-dev/hadoop/artifacts/tez-0.8.2.tar.gz
hadoop fs -mkdir -p /apps/tez || true
rm -rf $TEZ_TARBALL || true
echo "downloading tez tarball from sftp"
chmod 600 $WSHOME/le-dev/sftpdevkey
scp -i $WSHOME/le-dev/sftpdevkey sftpdev@10.41.1.31:/artifactory/tez-0.8.2.tar.gz $TEZ_TARBALL
hdfs dfs -put -f $TEZ_TARBALL /apps/tez || true
rm -rf $TEZ_TARBALL || true

DYNAMO_ARTIFACT_DIR=$WSHOME/le-dev/dynamo/artifacts
if [ -f $DYNAMO_ARTIFACT_DIR/dynamodb_local_latest.tar.gz ]; then
    echo "Skipping download of Dynamo"
else
    echo "Downloading Dynamo"
    pushd $DYNAMO_ARTIFACT_DIR
    wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
    popd
fi

if [ -d $DYNAMO_HOME ]; then
    echo "Removing old installation directory"
    rm -rf $DYNAMO_HOME
fi

mkdir -p $DYNAMO_HOME
pushd $DYNAMO_HOME
echo "Installing DynamoDB to $DYNAMO_HOME"
tar xzf $DYNAMO_ARTIFACT_DIR/dynamodb_local_latest.tar.gz 

CONDA_ARTIFACT_DIR=$WSHOME/le-dev/conda/artifacts
mkdir -p $CONDA_ARTIFACT_DIR
if [ -f $CONDA_ARTIFACT_DIR/Anaconda2-4.3.0-Linux-x86_64.sh ]; then
    echo "Skipping download of Anaconda"
elif [ -f $ANACONDA_HOME ]; then
    echo "Skipping installation of Anaconda"
else
    echo "Downloading Anaconda"
    pushd $CONDA_ARTIFACT_DIR
    wget https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh
    bash $CONDA_ARTIFACT_DIR/Anaconda2-4.3.0-Linux-x86_64.sh -b -p $ANACONDA_HOME
    popd
fi
$ANACONDA_HOME/bin/conda create -n lattice -y python=2.7.13 pip
source $ANACONDA_HOME/bin/activate lattice
pip install avro==1.7.7 fastavro==0.7.7 kazoo==2.2.1 patsy==0.3.0 pexpect==4.0.1 psutil==2.2.1 ptyprocess==0.5.1 python-dateutil==2.4.1
$ANACONDA_HOME/bin/conda install -y pandas=0.13.1=np18py27_0
pip install statsmodels==0.5.0
$ANACONDA_HOME/bin/conda install -y libiconv=1.14=0 libxml2=2.9.4=0 libxslt=1.1.28=3 lxml=3.4.0=py27_0 numpy=1.8.2=py27_1 openssl=1.0.2j=0 py=1.4.31=py27_0 pytest=2.9.2=py27_0 pytz=2016.10=py27_0 readline=6.2=2 scikit-learn=0.14.1=np18py27_1 setuptools=27.2.0=py27_0 sqlite=3.13.0=0 tk=8.5.18=0 wheel=0.29.0=py27_0 zlib=1.2.8=3

 


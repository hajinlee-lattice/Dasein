#!/bin/bash
echo "start.sh:"

if [ "${ANACONDA_HOME}" = "" ]; then
    ANACONDA_HOME=/opt/conda
fi
echo "  * ANACONDA_HOME: ${ANACONDA_HOME}"

echo "  * Anaconda env: ${CONDA_ENV}"
source activate ledatascience


#The following are required env variables set by caller. Here's samples for testing purpose.
#export CLIENT_ID="test_ds_server"
#export ZK_SERVER="127.0.0.1"
#export ZK_PORT="2181"
#export HDFS_SERVER="127.0.0.1"
#export HDFS_PORT="57000"
#export HDFS_USER="bross"

echo "datasciencelauncher.py" 

python /ledatascience/modelquality/datasciencelauncher.py 


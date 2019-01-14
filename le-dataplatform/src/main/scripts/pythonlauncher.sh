#!/bin/bash
echo "pythonlauncher.sh:"

if [[ -z "${ANACONDA_HOME}" ]]; then
    ANACONDA_HOME=/opt/conda
fi
echo "  * ANACONDA_HOME: ${ANACONDA_HOME}"
echo "  * AWS_BATCH_JOB_ID: ${AWS_BATCH_JOB_ID}"

echo "  * Activating Anaconda Env: $1"
source ${ANACONDA_HOME}/bin/activate $1

echo "  * Excuting Python Script: ${@:2}"
python "${@:2}"

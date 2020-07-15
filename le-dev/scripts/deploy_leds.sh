#!/usr/bin/env bash

function upload_artifact() {
    ARTIFACT=$1
    DS_ROOT=$2
    MD5SUM="${ARTIFACT_DIR}/${ARTIFACT}.md5"
    TO_UPLOAD="false"

    if [[ "${ARTIFACT}" == "dpconda" ]] && [[ -z `${ANACONDA_HOME}/bin/conda env list | grep "lattice "` ]]; then
        echo "Did not find lattice conda env, creating one"
        return 1
    fi

    if [[ "${ARTIFACT}" == "spark"* ]]; then
        S3_DIR=${S3_DIR_1}
    else
        S3_DIR=${S3_DIR_0}
    fi

    hdfs dfs -copyToLocal ${DS_ROOT}/${ARTIFACT}.md5 ${MD5SUM}.hdfs
    if [[ -f "${MD5SUM}.hdfs" ]]; then
        aws s3 cp s3://${S3_BUCKET}/${S3_DIR}/${ARTIFACT}.md5 ${MD5SUM}.s3
        if cmp -s "${MD5SUM}.hdfs" "${MD5SUM}.s3" ; then
           echo "Nothing changed for ${ARTIFACT}"
           TO_UPLOAD="false"
        else
           echo "Something changed for ${ARTIFACT}, re-download"
           TO_UPLOAD="true"
        fi
    else
        TO_UPLOAD="true"
    fi

    if [[ ${TO_UPLOAD} == "true" ]]; then
        rm -rf ${ARTIFACT_DIR}/${ARTIFACT}*
        aws s3 cp s3://${S3_BUCKET}/${S3_DIR}/${ARTIFACT} ${ARTIFACT_DIR}/${ARTIFACT}
        aws s3 cp s3://${S3_BUCKET}/${S3_DIR}/${ARTIFACT}.md5 ${ARTIFACT_DIR}/${ARTIFACT}.md5
        return 1
    else
        return 0
    fi
}

source "${WSHOME}/le-dev/scripts/check_aws_creds_expiration.sh"
check_aws_creds_expiration

ARTIFACT_DIR=${WSHOME}/le-dev/artifacts/leds
if [[ -d "${ARTIFACT_DIR}" ]]; then
    rm -rf ${ARTIFACT_DIR}/*
else
    mkdir -p ${ARTIFACT_DIR}
fi

P3_LEDS_VERSION=$(cat ${WSHOME}/le-config/conf/env/dev/latticeengines.properties | grep hadoop.leds.version= | cut -d= -f 2)
echo "P3_LEDS_VERSION=${P3_LEDS_VERSION}"
P2_LEDS_VERSION=$(cat ${WSHOME}/le-config/conf/env/dev/latticeengines.properties | grep hadoop.leds.version.p2= | cut -d= -f 2)
echo "P2_LEDS_VERSION=${P2_LEDS_VERSION}"

for LEDS_VERSION in "${P3_LEDS_VERSION}" "${P2_LEDS_VERSION}"; do
  DS_ROOT="/datascience/${LEDS_VERSION}"

  S3_BUCKET="latticeengines-dev-buildartifacts"
  if [[ ${LEDS_VERSION} =~ .*SNAPSHOT ]]; then
      echo "Downloading snapshot ${LEDS_VERSION} ..."
      S3_DIR_0="snapshot/sklearn-pipeline/v${LEDS_VERSION}"
      S3_DIR_1="snapshot/spark-scripts/v${LEDS_VERSION}"
  else
      echo "Downloading release ${LEDS_VERSION} ..."
      S3_DIR_0="release/sklearn-pipeline/v${LEDS_VERSION}"
      S3_DIR_1="release/spark-scripts/v${LEDS_VERSION}"
  fi

  for params in 'rfmodel|dataplatform|scripts' 'evmodel|playmaker|evmodel' 'scoring|scoring|scripts' 'dpconda|na|na' 'spark|spark|scripts'; do
      artifact=$(echo ${params} | cut -d \| -f 1)
      dir1=$(echo ${params} | cut -d \| -f 2)
      dir2=$(echo ${params} | cut -d \| -f 3)
      if upload_artifact ${artifact}-${LEDS_VERSION}.zip ${DS_ROOT}; then
          echo "No need to upload ${artifact}"
      else
          echo "Need to upload ${artifact}"
          pushd ${ARTIFACT_DIR} || exit
          unzip ${artifact}-${LEDS_VERSION}.zip
          if [[ ${artifact} == "dpconda" ]]; then
              if [[ ${LEDS_VERSION} < "1.6" ]]; then
                # the original conda install script does not work now, because libraries are removed from conda
                # use pre-build zip instead
                if [[ -d "/opt/conda/envs/lattice" ]]; then
                  rm -rf /opt/conda/envs/lattice
                fi
                if [[ $(uname) == "Darwin" ]]; then
                  aws s3 cp s3://latticeengines-test-artifacts/artifacts/conda/lattice.zip .
                  unzip -q lattice.zip -d /opt/conda/envs/
                else
                  aws s3 cp s3://latticeengines-dev-chef/conda/lattice_20181019.zip .
                  if [[ -d "/opt/conda/envs/lattice_20181019" ]]; then
                    rm -rf /opt/conda/envs/lattice_20181019
                  fi
                  unzip -q lattice_20181019.zip -d /opt/conda/envs/
                  mv /opt/conda/envs/lattice_20181019 /opt/conda/envs/lattice
                fi
              else
                pushd dpconda || exit
                bash setupenv_conda.sh
                popd || exit
              fi
          else
              hdfs dfs -rm -r -f ${DS_ROOT}/${dir1} || true
              hdfs dfs -mkdir -p ${DS_ROOT}/${dir1}
              hdfs dfs -copyFromLocal ${artifact} ${DS_ROOT}/${dir1}/${dir2}
          fi
          hdfs dfs -put -f ${ARTIFACT_DIR}/${artifact}-${LEDS_VERSION}.zip.md5 ${DS_ROOT}/
          popd || exit
      fi
  done

done



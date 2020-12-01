printf "%s\n" "${PRESTO_HOME:?You must set PRESTO_HOME}"

BOOTSTRAP_MODE=$1

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping Presto ..."

    PRESTO_VERSION=0.242
    ARTIFACT_DIR="$WSHOME/le-dev/artifacts"

    if [[ ! -f "${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}.tar.gz" ]]; then
        TGZ_URL="https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"
        wget --no-check-certificate ${TGZ_URL} -O ${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}.tar.gz
    fi

    if [[ -d "${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}" ]]; then
        rm -rf "${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}"
    fi
    tar -xzf "${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}.tar.gz" -C "${ARTIFACT_DIR}"

    if [[ -d "${PRESTO_HOME}" ]]; then
        sudo rm -rf "${PRESTO_HOME}"
    fi
    sudo mv "${ARTIFACT_DIR}/presto-server-${PRESTO_VERSION}" "${PRESTO_HOME}"

    if [[ ! -f "${ARTIFACT_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar" ]]; then
        TGZ_URL="https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar"
        wget --no-check-certificate ${TGZ_URL} -O ${ARTIFACT_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar
    fi

    cp ${ARTIFACT_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar ${PRESTO_HOME}/bin/presto-cli
    chmod +x ${PRESTO_HOME}/bin/presto-cli
fi

sudo chown -R ${USER} ${PRESTO_HOME}
if [[ -d "${PRESTO_HOME}/data" ]]; then
    rm -rf "${PRESTO_HOME}/data"
fi
mkdir ${PRESTO_HOME}/data

if [[ -d "${PRESTO_HOME}/etc" ]]; then
    rm -rf "${PRESTO_HOME}/etc"
fi
mkdir ${PRESTO_HOME}/etc

touch ${PRESTO_HOME}/etc/node.properties
cp "${WSHOME}/le-dev/presto/jvm.config" "${PRESTO_HOME}/etc"
cp "${WSHOME}/le-dev/presto/config.properties" "${PRESTO_HOME}/etc"
cp "${WSHOME}/le-dev/presto/node.properties" "${PRESTO_HOME}/etc"
echo "node.data-dir=${PRESTO_HOME}/data" >> "${PRESTO_HOME}/etc/node.properties"

mkdir "${PRESTO_HOME}/etc/catalog"
cp "${WSHOME}/le-dev/presto/hive.properties" "${PRESTO_HOME}/etc/catalog"

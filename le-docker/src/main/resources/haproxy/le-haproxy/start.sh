#!/usr/bin/env bash

function replace_token() {
    SERVER=$1
    VALUE=$2

    if [ ! -z "${VALUE}" ]; then
        HOSTPORTS=""
        for hp in $(echo $VALUE | sed "s/,/ /g")
        do
            if [ ! -z "${hp}" ]; then
                echo "add ${hp} to ${SERVER}"
                h=$(echo ${hp} | cut -d : -f 1)
                HOSTPORTS="${HOSTPORTS}\n  server ${h} ${hp} check"
            fi
        done
        sed -i "s/#{{${SERVER}}}/${HOSTPORTS}/" /usr/local/etc/haproxy/haproxy.cfg
    fi

}

if [ ! -z "${HOSTS}" ]; then

    SWAGGER_HOSTPORTS=""
    PLS_HOSTPORTS=""
    ADMIN_HOSTPORTS=""
    SCORINGAPI_HOSTPORTS=""
    MATCHAPI_HOSTPORTS=""
    OAUTH2_HOSTPORTS=""
    PLAYMAKER_HOSTPORTS=""
    EAI_HOSTPORTS=""
    METADATA_HOSTPORTS=""
    SCORING_HOSTPORTS=""
    MODELING_HOSTPORT=""
    DATAFLOWAPI_HOSTPORTS=""
    WORKFLOWAPI_HOSTPORTS=""
    QUATZ_HOSTPORTS=""
    MODELQUALITY_HOSTPORTS=""
    PROPDATA_HOSTPORTS=""
    DELLEBI_HOSTPORTS=""

    for h in $(echo $HOSTS | sed "s/,/ /g")
        do
            echo "add ${h} to server pools"
            SWAGGER_HOSTPORTS="${SWAGGER_HOSTPORTS}${h}:8080,"
            PLS_HOSTPORTS="${PLS_HOSTPORTS}${h}:8081,"
            ADMIN_HOSTPORTS="${ADMIN_HOSTPORTS}${h}:8085,"
            SCORINGAPI_HOSTPORTS="${SCORINGAPI_HOSTPORTS}${h}:8073,"
            MATCHAPI_HOSTPORTS="${MATCHAPI_HOSTPORTS}${h}:8076,"
            OAUTH2_HOSTPORTS="${OAUTH2_HOSTPORTS}${h}:8072,"
            PLAYMAKER_HOSTPORTS="${PLAYMAKER_HOSTPORTS}${h}:8071,"
            EAI_HOSTPORTS="${EAI_HOSTPORTS}${h}:9001,"
            METADATA_HOSTPORTS="${METADATA_HOSTPORTS}${h}:9002,"
            SCORING_HOSTPORTS="${SCORING_HOSTPORTS}${h}:9003,"
            MODELING_HOSTPORT="${MODELING_HOSTPORT}${h}:9004,"
            DATAFLOWAPI_HOSTPORTS="${DATAFLOWAPI_HOSTPORTS}${h}:9005,"
            WORKFLOWAPI_HOSTPORTS="${WORKFLOWAPI_HOSTPORTS}${h}:9006,"
            QUATZ_HOSTPORTS="${QUATZ_HOSTPORTS}${h}:9007,"
            MODELQUALITY_HOSTPORTS="${MODELQUALITY_HOSTPORTS}${h}:9008,"
            PROPDATA_HOSTPORTS="${PROPDATA_HOSTPORTS}${h}:9009,"
            DELLEBI_HOSTPORTS="${DELLEBI_HOSTPORTS}${h}:9010,"
        done

fi

replace_token swagger ${SWAGGER_HOSTPORTS}
replace_token pls ${PLS_HOSTPORTS}
replace_token admin ${ADMIN_HOSTPORTS}
replace_token scoringapi ${SCORINGAPI_HOSTPORTS}
replace_token matchapi ${MATCHAPI_HOSTPORTS}
replace_token oauth2 ${OAUTH2_HOSTPORTS}
replace_token playmaker ${PLAYMAKER_HOSTPORTS}
replace_token eai ${EAI_HOSTPORTS}
replace_token metadata ${METADATA_HOSTPORTS}
replace_token scoring ${SCORING_HOSTPORTS}
replace_token modeling ${MODELING_HOSTPORT}
replace_token dataflowapi ${DATAFLOWAPI_HOSTPORTS}
replace_token workflowapi ${WORKFLOWAPI_HOSTPORTS}
replace_token quartz ${QUATZ_HOSTPORTS}
replace_token modelquality ${MODELQUALITY_HOSTPORTS}
replace_token propdata ${PROPDATA_HOSTPORTS}
replace_token dellebi ${DELLEBI_HOSTPORTS}

cat /usr/local/etc/haproxy/haproxy.cfg

/docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg

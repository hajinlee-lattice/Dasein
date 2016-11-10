#!/usr/bin/env bash

if [ ! -z "${SWAGGER_HOSTPORT}" ]; then
    sed -i "s/#{{SWAGGER_HOSTPORT}}/  server swagger ${SWAGGER_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${PLS_HOSTPORT}" ]; then
    sed -i "s/#{{PLS_HOSTPORT}}/  server pls ${PLS_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${ADMIN_HOSTPORT}" ]; then
    sed -i "s/#{{ADMIN_HOSTPORT}}/  server admin ${ADMIN_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${SCORINGAPI_HOSTPORT}" ]; then
    sed -i "s/#{{SCORINGAPI_HOSTPORT}}/  server scoringapi ${SCORINGAPI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${MATCHAPI_HOSTPORT}" ]; then
    sed -i "s/#{{MATCHAPI_HOSTPORT}}/  server matchapi ${MATCHAPI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${OAUTH2_HOSTPORT}" ]; then
    sed -i "s/#{{OAUTH2_HOSTPORT}}/  server oauth2 ${OAUTH2_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${PLAYMAKER_HOSTPORT}" ]; then
    sed -i "s/#{{PLAYMAKER_HOSTPORT}}/  server playmaker ${PLAYMAKER_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi


if [ ! -z "${EAI_HOSTPORT}" ]; then
    sed -i "s/#{{EAI_HOSTPORT}}/  server eai ${EAI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${METADATA_HOSTPORT}" ]; then
    sed -i "s/#{{METADATA_HOSTPORT}}/  server metadata ${METADATA_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${SCORING_HOSTPORT}" ]; then
    sed -i "s/#{{SCORING_HOSTPORT}}/  server scoring ${SCORING_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${MODELING_HOSTPORT}" ]; then
    sed -i "s/#{{MODELING_HOSTPORT}}/  server modeling ${MODELING_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${DATAFLOWAPI_HOSTPORT}" ]; then
    sed -i "s/#{{DATAFLOWAPI_HOSTPORT}}/  server dataflowapi ${DATAFLOWAPI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${WORKFLOWAPI_HOSTPORT}" ]; then
    sed -i "s/#{{WORKFLOWAPI_HOSTPORT}}/  server workflowapi ${WORKFLOWAPI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${QUATZ_HOSTPORT}" ]; then
    sed -i "s/#{{QUATZ_HOSTPORT}}/  server quartz ${QUATZ_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${MODELQUALITY_HOSTPORT}" ]; then
    sed -i "s/#{{MODELQUALITY_HOSTPORT}}/  server modelquality ${MODELQUALITY_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${PROPDATA_HOSTPORT}" ]; then
    sed -i "s/#{{PROPDATA_HOSTPORT}}/  server propdata ${PROPDATA_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi

if [ ! -z "${DELLEBI_HOSTPORT}" ]; then
    sed -i "s/#{{DELLEBI_HOSTPORT}}/  server dellebi ${DELLEBI_HOSTPORT} check/" /usr/local/etc/haproxy/haproxy.cfg
fi


/docker-entrypoint.sh haproxy -f /usr/local/etc/haproxy/haproxy.cfg

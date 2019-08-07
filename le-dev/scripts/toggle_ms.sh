printf "%s\n" "${MSTOOL_HOME:?You must set MSTOOL_HOME}"


ANACONDA_MINISTACK_PATH=${ANACONDA_MINISTACK_PATH:=${ANACONDA_HOME}/envs/ministack}
${ANACONDA_MINISTACK_PATH}/bin/python ${MSTOOL_HOME}/bin/toggle_services.py $@

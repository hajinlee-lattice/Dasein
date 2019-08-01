printf "%s\n" "${MSTOOL_HOME:?You must set MSTOOL_HOME}"

${ANACONDA_HOME}/envs/ministack/bin/python ${MSTOOL_HOME}/bin/setup_ministack.py $@

package com.latticeengines.yarn.exposed.runtime;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;

public class LedpAppmasterServiceImpl extends LedpAppmasterService {

    private static final Logger log = LoggerFactory.getLogger(LedpAppmasterServiceImpl.class);

    private Exception e;

    @Override
    public void handleException(Exception e) {
        log.error(e.getMessage(), e);
        this.e = e;
    }

    @Override
    protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
        log.info("Send out messages from Appmaster Service !!");
        if (e != null) {
            return getConversionService().convert(
                    new AppMasterServiceResponse(e.getMessage() + ":  " + ExceptionUtils.getStackTrace(e)),
                    MindRpcMessageHolder.class);
        }
        return getConversionService().convert(new AppMasterServiceResponse("found no error !"),
                MindRpcMessageHolder.class);
    }

}

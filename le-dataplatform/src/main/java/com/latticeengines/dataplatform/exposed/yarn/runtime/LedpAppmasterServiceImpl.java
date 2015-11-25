package com.latticeengines.dataplatform.exposed.yarn.runtime;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Component;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;

@Component("ledpAppmasterService")
public class LedpAppmasterServiceImpl extends LedpAppmasterService {

    private Exception e;

    @Override
    public void handleException(Exception e) {
        log.error(e);
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

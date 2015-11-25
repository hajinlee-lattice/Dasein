package com.latticeengines.dataplatform.exposed.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;

public abstract class LedpAppmasterService extends MindAppmasterService implements SmartLifecycle {

    protected static Log log = LogFactory.getLog(LedpAppmasterService.class);

    @Override
    protected abstract MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message);

    @Override
    public String toString() {
        return "Port is :" + getPort();
    }

    public abstract void handleException(Exception e);

}

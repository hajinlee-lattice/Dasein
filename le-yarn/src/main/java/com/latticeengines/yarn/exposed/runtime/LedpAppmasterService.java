package com.latticeengines.yarn.exposed.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;

public abstract class LedpAppmasterService extends MindAppmasterService implements SmartLifecycle {

    protected static Logger log = LoggerFactory.getLogger(LedpAppmasterService.class);

    @Override
    protected abstract MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message);

    @Override
    public String toString() {
        return "Port is :" + getPort();
    }

    public abstract void handleException(Exception e);

}

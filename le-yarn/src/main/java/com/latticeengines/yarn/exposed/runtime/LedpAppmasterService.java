package com.latticeengines.yarn.exposed.runtime;

import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;

public abstract class LedpAppmasterService extends MindAppmasterService implements SmartLifecycle {

    @Override
    protected abstract MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message);

    @Override
    public String toString() {
        return "Port is :" + getPort();
    }

    public abstract void handleException(Exception e);

}

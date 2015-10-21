package com.latticeengines.propdata.api.testframework;

import org.springframework.beans.factory.annotation.Value;

public abstract class PropDataApiDeploymentTestNGBase extends PropDataApiAbstractTestNGBase {

    @Value("${propdata.api.deployment.hostport}")
    private String hostPort;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

}

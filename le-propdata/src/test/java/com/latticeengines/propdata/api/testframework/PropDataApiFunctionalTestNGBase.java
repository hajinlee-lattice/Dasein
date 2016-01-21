package com.latticeengines.propdata.api.testframework;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

public abstract class PropDataApiFunctionalTestNGBase extends PropDataApiAbstractTestNGBase {
	
	protected RestTemplate restTemplate = new RestTemplate();

    @Value("${propdata.api.functional.hostport}")
    private String hostPort;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

}

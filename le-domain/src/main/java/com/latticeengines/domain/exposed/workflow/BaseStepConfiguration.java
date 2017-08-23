package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class BaseStepConfiguration {

    @JsonProperty("internal_resource_host_port")
    private String internalResourceHostPort;

    @JsonProperty("skip_step")
    private boolean skipStep = false;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public boolean isSkipStep() {
        return skipStep;
    }

    public void setSkipStep(boolean skipStep) {
        this.skipStep = skipStep;
    }
}

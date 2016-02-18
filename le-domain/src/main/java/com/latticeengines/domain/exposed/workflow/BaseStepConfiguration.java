package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class BaseStepConfiguration {
    private String internalResourceHostPort;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("internal_resource_host_port")
    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    @JsonProperty("internal_resource_host_port")
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }
}

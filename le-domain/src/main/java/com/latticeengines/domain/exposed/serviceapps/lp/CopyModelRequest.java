package com.latticeengines.domain.exposed.serviceapps.lp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CopyModelRequest {

    @JsonProperty("TargetTenant")
    private String targetTenant;

    @JsonProperty("ModelGUID")
    private String modelGuid;

    @JsonProperty("Async")
    private String async;

    public String getTargetTenant() {
        return targetTenant;
    }

    public void setTargetTenant(String targetTenant) {
        this.targetTenant = targetTenant;
    }

    public String getModelGuid() {
        return modelGuid;
    }

    public void setModelGuid(String modelGuid) {
        this.modelGuid = modelGuid;
    }

    public String getAsync() {
        return async;
    }

    public void setAsync(String async) {
        this.async = async;
    }

}

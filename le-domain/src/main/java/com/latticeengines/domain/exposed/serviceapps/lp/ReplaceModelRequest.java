package com.latticeengines.domain.exposed.serviceapps.lp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ReplaceModelRequest {

    @JsonProperty("SourceTenant")
    private String sourceTenant;

    @JsonProperty("SourceModelGuid")
    private String sourceModelGuid;

    @JsonProperty("TargetTenant")
    private String targetTenant;

    @JsonProperty("TargetModelGuid")
    private String targetModelGuid;

    public String getSourceTenant() {
        return sourceTenant;
    }

    public void setSourceTenant(String sourceTenant) {
        this.sourceTenant = sourceTenant;
    }

    public String getSourceModelGuid() {
        return sourceModelGuid;
    }

    public void setSourceModelGuid(String sourceModelGuid) {
        this.sourceModelGuid = sourceModelGuid;
    }

    public String getTargetTenant() {
        return targetTenant;
    }

    public void setTargetTenant(String targetTenant) {
        this.targetTenant = targetTenant;
    }

    public String getTargetModelGuid() {
        return targetModelGuid;
    }

    public void setTargetModelGuid(String targetModelGuid) {
        this.targetModelGuid = targetModelGuid;
    }
}

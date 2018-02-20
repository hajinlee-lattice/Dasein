package com.latticeengines.domain.exposed.serviceapps.core;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttrConfigProp implements Serializable {
    private static final long serialVersionUID = 3252663465868093601L;

    @JsonProperty("AllowCustomization")
    private boolean allowCustomization;

    @JsonProperty("SystemValue")
    private Serializable systemValue;

    @JsonProperty("CustomValue")
    private Serializable customValue;

    public boolean isAllowCustomization() {
        return allowCustomization;
    }

    public void setAllowCustomization(boolean allowCustomization) {
        this.allowCustomization = allowCustomization;
    }

    public Serializable getSystemValue() {
        return systemValue;
    }

    public void setSystemValue(Serializable systemValue) {
        this.systemValue = systemValue;
    }

    public Serializable getCustomValue() {
        return customValue;
    }

    public void setCustomValue(Serializable customValue) {
        this.customValue = customValue;
    }
}

package com.latticeengines.domain.exposed.serviceapps.core;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class AttrConfigProp<T extends Serializable> implements Serializable {
    private static final long serialVersionUID = 3252663465868093601L;

    @JsonProperty("AllowCustomization")
    private Boolean allowCustomization;

    @JsonProperty("SystemValue")
    private T systemValue;

    @JsonProperty("CustomValue")
    private T customValue;

    public Boolean isAllowCustomization() {
        return allowCustomization;
    }

    public void setAllowCustomization(Boolean allowCustomization) {
        this.allowCustomization = allowCustomization;
    }

    public T getSystemValue() {
        return systemValue;
    }

    public void setSystemValue(T systemValue) {
        this.systemValue = systemValue;
    }

    public T getCustomValue() {
        return customValue;
    }

    public void setCustomValue(T customValue) {
        this.customValue = customValue;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof AttrConfigProp)) {
            return false;
        }
        AttrConfigProp obj = (AttrConfigProp) o;
        return allowCustomization == obj.isAllowCustomization()
                && ((systemValue == obj.getSystemValue())
                        || (systemValue != null && systemValue.equals(obj.getSystemValue())))
                && (customValue == obj.getCustomValue()
                        || (customValue != null && (customValue.equals(obj.getCustomValue()))));
    }
}

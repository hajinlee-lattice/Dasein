package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataRule {

    private String name;

    private String description;

    private Map<String, String> properties;

    private boolean enabled;

    private boolean isFrozenEnablement;

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty
    public String getDescription() {
        return description;
    }

    @JsonProperty
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @JsonProperty
    public boolean isEnabled() {
        return enabled;
    }

    @JsonProperty
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @JsonProperty
    public boolean isFrozenEnablement() {
        return isFrozenEnablement;
    }

    @JsonProperty
    public void setFrozenEnablement(boolean isFrozenEnablement) {
        this.isFrozenEnablement = isFrozenEnablement;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataRule other = (DataRule) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

}

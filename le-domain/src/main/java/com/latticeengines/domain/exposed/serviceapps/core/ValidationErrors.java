package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidationErrors {

    @JsonProperty("errors")
    private Map<Type, List<String>> errors = new HashMap<>();

    public Map<Type, List<String>> getErrors() {
        return errors;
    }

    public void setErrors(Map<Type, List<String>> errors) {
        this.errors = errors;
    }

    public enum Type {
        EXCEED_SYSTEM_LIMIT, //
        EXCEED_DATA_LICENSE, //
        EXCEED_USAGE_LIMIT, //
        INVALID_ACTIVATION, //
        INVALID_USAGE_CHANGE, //
        INVALID_PROP_CHANGE, //
        OTHER;
    }
}

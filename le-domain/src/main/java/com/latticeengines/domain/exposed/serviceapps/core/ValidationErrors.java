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
        EXCEED_SYSTEM_LIMIT("System limit exceeded: %d attributes"), //
        EXCEED_DATA_LICENSE("Data license limit exceeded: %d attributes"), //
        EXCEED_USAGE_LIMIT("Usage limit exceeded: %d attributes"), //
        INVALID_ACTIVATION("Cannot activate deprecated attribute: %d attributes"), //
        INVALID_USAGE_CHANGE("Usage change is not allowed: %d attributes"), //
        INVALID_PROP_CHANGE("Customization is not allowed: %d attributes"), //
        DUPLICATE_NAME_CHANGE("Duplicated names found: %d attributes");

        private String message;

        Type(String msg) {
            this.message = msg;
        }

        public String getMessage() {
            return this.message;
        }

    }
}

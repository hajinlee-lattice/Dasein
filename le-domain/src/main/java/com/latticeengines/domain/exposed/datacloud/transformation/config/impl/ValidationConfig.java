package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidationConfig {

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Filter")
    private String filter;

    @JsonProperty("Validator")
    private String validator;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getValidator() {
        return validator;
    }

    public void setValidator(String validator) {
        this.validator = validator;
    }
}

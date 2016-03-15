package com.latticeengines.domain.exposed.exception;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExceptionHandlerErrors {

    @JsonProperty("error")
    private String error;

    @JsonProperty("error_description")
    private String description;

    @JsonProperty("errors")
    private List<String> errors = new ArrayList<>();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}

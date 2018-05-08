package com.latticeengines.domain.exposed.ulysses;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.exception.ErrorDetails;

public class FrontEndResponse<T> {
    @JsonProperty("Success")
    public boolean success;

    @JsonProperty("Errors")
    public Map<String, String> errors;

    @JsonProperty("Result")
    public T result;

    public FrontEndResponse() {
        this.success = true;
        this.errors = new HashMap<>();
    }

    public FrontEndResponse(T result) {
        this.result = result;
        this.success = true;
        this.errors = new HashMap<>();
    }

    public FrontEndResponse(ErrorDetails error) {
        this.success = false;
        this.errors = new HashMap<>();
        this.errors.put(error.getErrorCode().toString(), error.getErrorMsg());
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Map<String, String> getErrors() {
        return errors;
    }

    public void setErrors(Map<String, String> errors) {
        this.errors = errors;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}

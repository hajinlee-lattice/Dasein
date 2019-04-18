package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ErrorDetails;

public class FrontEndResponse<T> {
    @JsonProperty("Success")
    public boolean success;

    @JsonProperty("Errors")
    public List<KeyValuePair> errors;

    @JsonProperty("Result")
    public T result;

    public FrontEndResponse() {
        this.success = true;
        this.errors = new ArrayList<>();
    }

    public FrontEndResponse(T result) {
        this.result = result;
        this.success = true;
        this.errors = new ArrayList<>();
    }

    public FrontEndResponse(ErrorDetails error) {
        this.success = false;
        this.errors = new ArrayList<>();
        errors.add(new KeyValuePair(error.getErrorCode().toString(), error.getErrorMsg()));
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public List<KeyValuePair> getErrors() {
        return errors;
    }

    public void setErrors(List<KeyValuePair> errors) {
        this.errors = errors;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    private class KeyValuePair {
        @JsonProperty("Key")
        private String key;

        @JsonProperty("Value")
        private String value;

        KeyValuePair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @SuppressWarnings("unused")
        public String getKey() {
            return key;
        }

        @SuppressWarnings("unused")
        public void setKey(String key) {
            this.key = key;
        }

        @SuppressWarnings("unused")
        public String getValue() {
            return value;
        }

        @SuppressWarnings("unused")
        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this);
        }
    }
}

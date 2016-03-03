package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;

public class InputValidatorWrapper implements Serializable {
    @JsonProperty("raw_type")
    private String rawType;

    @JsonProperty("json")
    private String json;

    public InputValidatorWrapper(InputValidator validator) {
        setValidator(validator);
    }

    public InputValidatorWrapper() {
    }

    @JsonIgnore
    public Class<?> getType() {
        try {
            return Class.forName(rawType);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Could not locate class with name %s", rawType), e);
        }
    }

    @Transient
    @JsonIgnore
    public InputValidator getValidator() {
        if (json == null || rawType == null) {
            return null;
        }
        return (InputValidator) JsonUtils.deserialize(json, getType());
    }

    @Transient
    @JsonIgnore
    void setValidator(InputValidator validator) {
        if (validator == null) {
            return;
        }
        rawType = validator.getClass().getName();
        json = JsonUtils.serialize(validator);
    }
}
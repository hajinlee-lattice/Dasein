package com.latticeengines.domain.exposed.metadata;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;

public class TestValidator extends InputValidator {
    @JsonProperty("test")
    private String test;

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }

    @Override
    public boolean validate(String field, Map<String, String> row, Table metadata) {
        return true;
    }
}

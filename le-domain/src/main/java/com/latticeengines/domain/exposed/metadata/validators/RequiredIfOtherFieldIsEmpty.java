package com.latticeengines.domain.exposed.metadata.validators;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;

public class RequiredIfOtherFieldIsEmpty extends InputValidator {
    @JsonProperty
    public String otherField;

    public RequiredIfOtherFieldIsEmpty(String otherField) {
        this.otherField = otherField;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public RequiredIfOtherFieldIsEmpty() {
    }

    @Override
    public boolean validate(String field, Map<String, String> row, Table metadata) {
        Object value = row.get(metadata.getAttribute(field).getDisplayName());
        if (value == null || value.toString().equals("")) {
            Object otherFieldValue = row.get(metadata.getAttribute(otherField).getDisplayName());
            if (otherFieldValue == null || otherFieldValue.toString().equals("")) {
                return false;
            }
        }

        return true;
    }
}

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
    public boolean validate(String field, Map<String, Object> row, Table metadata) {
        if (otherField.equals(field)) {
            return true;
        }
        Object value = row.get(field);
        if (value == null || value.toString().equals("")) {
            Object otherFieldValue = row.get(otherField);
            if (otherFieldValue == null || value.toString().equals("")) {
                return false;
            }
        }

        return true;
    }
}

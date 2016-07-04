package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FieldMappingDocument {
    @JsonProperty
    private List<FieldMapping> fieldMappings;

    @JsonProperty
    private List<String> ignoredFields;

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public List<FieldMapping> getFieldMappings() {
        return this.fieldMappings;
    }

    public void setIgnoredFields(List<String> ignoredFields) {
        this.ignoredFields = ignoredFields;
    }

    public List<String> getIgnoredFields() {
        return this.ignoredFields;
    }
}

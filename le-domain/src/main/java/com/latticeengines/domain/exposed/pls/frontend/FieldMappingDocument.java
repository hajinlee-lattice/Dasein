package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

import java.util.List;

public class FieldMappingDocument {
    @JsonProperty
    private List<FieldMapping> fieldMappings;

    @JsonProperty
    private SchemaInterpretation schemaInterpretation;

    @JsonProperty
    private List<String> ignoredFields;

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public List<FieldMapping> getFieldMappings() {
        return this.fieldMappings;
    }

    public void setSchemaInterpretation(SchemaInterpretation schemaInterpretation) {
        this.schemaInterpretation = schemaInterpretation;
    }

    public SchemaInterpretation getSchemaInterpretation() {
        return this.schemaInterpretation;
    }

    public void setIgnoredFields(List<String> ignoredFields) {
        this.ignoredFields = ignoredFields;
    }

    public List<String> getIgnoredFields() {
        return this.ignoredFields;
    }
}

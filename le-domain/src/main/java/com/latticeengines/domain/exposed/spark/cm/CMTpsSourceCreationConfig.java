package com.latticeengines.domain.exposed.spark.cm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CMTpsSourceCreationConfig extends SparkJobConfig {

    public static final String NAME = "cmTpsSourceCreation";

    @JsonProperty("FieldMaps")
    List<FieldMapping> fieldMaps; // List of mapping between a inferred new standard field and existing fields

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<FieldMapping> getFieldMaps() {
        return fieldMaps;
    }

    public void setFieldMaps(List<FieldMapping> fieldMaps) {
        this.fieldMaps = fieldMaps;
    }

    public static class FieldMapping {
        @JsonProperty("NewStandardField")
        String newStandardField;

        @JsonProperty("SourceFields")
        List<String> sourceFields;

        public String getNewStandardField() {
            return newStandardField;
        }

        public void setNewStandardField(String newStandardField) {
            this.newStandardField = newStandardField;
        }

        public List<String> getSourceFields() {
            return sourceFields;
        }

        public void setSourceFields(List<String> sourceFields) {
            this.sourceFields = sourceFields;
        }
    }
}

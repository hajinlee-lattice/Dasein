package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishTextToSqlConfiguration extends PublishToSqlConfiguration {
    private String nullString;
    private String enclosedBy;
    private String optionalEnclosedBy;
    private String escapedBy;
    private String fieldTerminatedBy;
    private String lineTerminatedBy;

    @Override
    @JsonProperty("ConfigurationType")
    public String getConfigurationType() {
        return this.getClass().getSimpleName();
    }

    @JsonProperty("NullString")
    public String getNullString() {
        return nullString;
    }

    @JsonProperty("NullString")
    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

    @JsonProperty("EnclosedBy")
    public String getEnclosedBy() {
        return enclosedBy;
    }

    @JsonProperty("EnclosedBy")
    public void setEnclosedBy(String enclosedBy) {
        this.enclosedBy = enclosedBy;
    }

    @JsonProperty("OptionalEnclosedBy")
    public String getOptionalEnclosedBy() {
        return optionalEnclosedBy;
    }

    @JsonProperty("OptionalEnclosedBy")
    public void setOptionalEnclosedBy(String optionalEnclosedBy) {
        this.optionalEnclosedBy = optionalEnclosedBy;
    }

    @JsonProperty("EscapedBy")
    public String getEscapedBy() {
        return escapedBy;
    }

    @JsonProperty("EscapedBy")
    public void setEscapedBy(String escapedBy) {
        this.escapedBy = escapedBy;
    }

    @JsonProperty("FieldTerminatedBy")
    public String getFieldTerminatedBy() {
        return fieldTerminatedBy;
    }

    @JsonProperty("FieldTerminatedBy")
    public void setFieldTerminatedBy(String fieldTerminatedBy) {
        this.fieldTerminatedBy = fieldTerminatedBy;
    }

    @JsonProperty("LineTerminatedBy")
    public String getLineTerminatedBy() {
        return lineTerminatedBy;
    }

    @JsonProperty("LineTerminatedBy")
    public void setLineTerminatedBy(String lineTerminatedBy) {
        this.lineTerminatedBy = lineTerminatedBy;
    }

}

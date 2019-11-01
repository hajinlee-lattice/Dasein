package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldValidationMessage {
    // Internal name of the schema field that columns are mapped to.  Must be unique for each mapping.
    @JsonProperty
    private String fieldName;

    // The name of the mapped column in the imported files for this schema field.  Also know as display name.
    @JsonProperty
    private String columnName;

    public enum MessageLevel {
        WARNING,
        ERROR
    }

    public FieldValidationMessage (String fieldName, String columnName, String message,
                                   FieldValidationMessage.MessageLevel level) {
        this.fieldName = fieldName;
        this.columnName = columnName;
        this.message = message;
        this.messageLevel = level;
    }

    // The error message level.
    @JsonProperty
    private MessageLevel messageLevel;

    // The message string describing the problem.
    @JsonProperty
    String message;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public MessageLevel getMessageLevel() {
        return messageLevel;
    }

    public void setMessageLevel(MessageLevel messageLevel) {
        this.messageLevel = messageLevel;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("%s for field name %s, column name %s: %s", messageLevel.name(), fieldName, columnName,
                message);
    }
}

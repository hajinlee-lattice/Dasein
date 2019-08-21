package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValidateFieldDefinitionsResponse extends ValidateFieldDefinitionsRecord {

    public enum ValidationResult {
        PASS,
        WARNING,
        ERROR
    }

    // The overall validation response state, indicating if the changes are valid (PASS), valid but with minor
    // issues (WARNING), or invalid (ERROR).
    @JsonProperty
    private ValidationResult validationResult;

    // A map of all the warnings and errors organized by the field sections.
    @JsonProperty
    private Map<String, List<FieldValidationMessage>> fieldValidationMessagesMap = new HashMap<>();

    public ValidationResult getValidationResult() {
        return validationResult;
    }

    public void setValidationResult(ValidationResult validationResult) {
        this.validationResult = validationResult;
    }

    public Map<String, List<FieldValidationMessage>> getFieldValidationMessagesMap() {
        return fieldValidationMessagesMap;
    }

    public void setFieldValidationMessagesMap(Map<String, List<FieldValidationMessage>> fieldValidationMessagesMap) {
        this.fieldValidationMessagesMap = fieldValidationMessagesMap;
    }

    // Add the FieldValidationMessage record for one field.  Returns true if provided FieldValidationMessage list was
    // added to the map.
    public boolean addFieldValidationMessages(String fieldSectionName,
                                              List<FieldValidationMessage> fieldValidationMessages,
                                              boolean replaceExisting) {
        if (fieldValidationMessagesMap == null) {
            fieldValidationMessagesMap = new HashMap<>();
        }
        if (replaceExisting || !fieldValidationMessagesMap.containsKey(fieldSectionName)) {
            fieldValidationMessagesMap.put(fieldSectionName, fieldValidationMessages);
            return true;
        }
        return false;
    }

    // Get the field definitions record for a specific section.
    public List<FieldValidationMessage> getFieldValidationMessages(String fieldSectionName) {
        if (MapUtils.isNotEmpty(fieldValidationMessagesMap) &&
                fieldValidationMessagesMap.containsKey(fieldSectionName)) {
            return fieldValidationMessagesMap.get(fieldSectionName);
        }
        return null;
    }
}

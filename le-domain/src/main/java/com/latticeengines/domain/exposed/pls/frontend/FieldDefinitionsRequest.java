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
public class FieldDefinitionsRequest {

    // Contains the current state of the field definitions prior to the application of the requested changes.
    @JsonProperty
    protected Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap;

    public Map<String, List<FieldDefinition>> getFieldDefinitionsRecordsMap() {
        return fieldDefinitionsRecordsMap;
    }

    public void setFieldDefinitionsRecordsMap(Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap) {
        this.fieldDefinitionsRecordsMap = fieldDefinitionsRecordsMap;
    }

    // Add the FieldDefinition records for one field section.  Returns true if provided FieldDefinition list was added
    // to the map.
    public boolean addFieldDefinitionsRecords(String fieldSectionName, List<FieldDefinition> fieldDefinitionsRecords,
                                              boolean replaceExisting) {
        if (fieldDefinitionsRecordsMap == null) {
            fieldDefinitionsRecordsMap = new HashMap<>();
        }
        if (replaceExisting || !fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            fieldDefinitionsRecordsMap.put(fieldSectionName, fieldDefinitionsRecords);
            return true;
        }
        return false;
    }

    // Get the field definitions records for a specific section.
    public List<FieldDefinition> getFieldDefinitionsRecords(String fieldSectionName) {
        if (MapUtils.isNotEmpty(fieldDefinitionsRecordsMap) &&
                fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            return fieldDefinitionsRecordsMap.get(fieldSectionName);
        }
        return null;
    }
}

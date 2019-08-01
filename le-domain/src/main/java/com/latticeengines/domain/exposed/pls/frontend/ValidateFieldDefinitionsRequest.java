package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidateFieldDefinitionsRequest extends FieldDefinitionsRequest {

    // Contains the set of user requested changes to the field definitions.
    @JsonProperty
    private Map<String, List<FieldDefinition>> fieldDefinitionsChangesMap;

    public Map<String, List<FieldDefinition>> getFieldDefinitionsChangesMap() {
        return fieldDefinitionsChangesMap;
    }

    public void setFieldDefinitionsChangesMap(Map<String, List<FieldDefinition>> fieldDefinitionsChangesMap) {
        this.fieldDefinitionsChangesMap = fieldDefinitionsChangesMap;
    }

    // Add the FieldDefinition change for one field section.  Returns true if provided FieldDefinition list was added
    // to the map.
    public boolean addFieldDefinitionsChanges(String fieldSectionName, List<FieldDefinition> fieldDefinitionsChanges,
                                              boolean replaceExisting) {
        if (fieldDefinitionsChangesMap == null) {
            fieldDefinitionsChangesMap = new HashMap<>();
        }
        if (replaceExisting || !fieldDefinitionsChangesMap.containsKey(fieldSectionName)) {
            fieldDefinitionsChangesMap.put(fieldSectionName, fieldDefinitionsChanges);
            return true;
        }
        return false;
    }

    // Get the field definitions changes for a specific section.
    public List<FieldDefinition> getFieldDefinitionsChanges(String fieldSectionName) {
        if (MapUtils.isNotEmpty(fieldDefinitionsChangesMap) &&
                fieldDefinitionsChangesMap.containsKey(fieldSectionName)) {
            return fieldDefinitionsChangesMap.get(fieldSectionName);
        }
        return null;
    }
}

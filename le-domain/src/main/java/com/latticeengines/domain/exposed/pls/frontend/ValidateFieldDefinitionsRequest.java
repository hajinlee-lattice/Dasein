package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidateFieldDefinitionsRequest extends CommitFieldDefinitionsRequest {

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
        if (replaceExisting || !fieldDefinitionsChangesMap.containsKey(fieldSectionName)) {
            fieldDefinitionsChangesMap.put(fieldSectionName, fieldDefinitionsChanges);
            return true;
        }
        return false;
    }

    // Get the field definitions changes for a specific section.
    public List<FieldDefinition> getFieldDefinitionsChanges(String fieldSectionName) {
        if (fieldDefinitionsChangesMap.containsKey(fieldSectionName)) {
            return fieldDefinitionsChangesMap.get(fieldSectionName);
        }
        return null;
    }
}

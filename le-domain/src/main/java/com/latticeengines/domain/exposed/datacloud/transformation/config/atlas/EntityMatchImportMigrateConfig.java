package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EntityMatchImportMigrateConfig extends TransformerConfig {

    /**
     * Key: source, Value: destination
     */
    @JsonProperty("rename_map")
    private Map<String, String> renameMap;

    /**
     * Key: source, Value: destination
     */
    @JsonProperty("duplicate_map")
    private Map<String, String> duplicateMap;

    private List<String> retainFields;

    public Map<String, String> getRenameMap() {
        return renameMap;
    }

    public void setRenameMap(Map<String, String> renameMap) {
        this.renameMap = renameMap;
    }

    public Map<String, String> getDuplicateMap() {
        return duplicateMap;
    }

    public void setDuplicateMap(Map<String, String> duplicateMap) {
        this.duplicateMap = duplicateMap;
    }

    public List<String> getRetainFields() {
        return retainFields;
    }

    public void setRetainFields(List<String> retainFields) {
        this.retainFields = retainFields;
    }
}

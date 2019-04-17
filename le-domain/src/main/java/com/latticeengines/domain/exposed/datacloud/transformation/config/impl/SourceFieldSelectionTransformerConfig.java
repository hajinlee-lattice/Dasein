package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceFieldSelectionTransformerConfig extends TransformerConfig {

    @JsonProperty("NewFields")
    private List<String> newFields;

    @JsonProperty("RetainFields")
    private List<String> retainFields;

    @JsonProperty("RenameFieldMap")
    private Map<String, String> renameFieldMap;

    public List<String> getNewFields() {
        return newFields;
    }

    public void setNewFields(List<String> newFields) {
        this.newFields = newFields;
    }

    public List<String> getRetainFields() {
        return retainFields;
    }

    public void setRetainFields(List<String> retainFields) {
        this.retainFields = retainFields;
    }

    public Map<String, String> getRenameFieldMap() {
        return renameFieldMap;
    }

    public void setRenameFieldMap(Map<String, String> renameFieldMap) {
        this.renameFieldMap = renameFieldMap;
    }

}

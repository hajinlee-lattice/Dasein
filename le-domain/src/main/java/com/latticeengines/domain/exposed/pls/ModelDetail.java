package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;


public class ModelDetail {

    @JsonProperty("ModelSummary")
    private ModelSummary modelSummary;

    @JsonProperty("Categories")
    private Map<String, CategoryObject> categories = new HashMap<>();

    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public Map<String, CategoryObject> getCategories() {
        return categories;
    }

    public void setCategories(Map<String, CategoryObject> categories) {
        this.categories = categories;
    }

}

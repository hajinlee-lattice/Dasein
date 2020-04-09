package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitSystemBatchStoreConfig extends SparkJobConfig {
    public static final String NAME = "splitSystemBatchStore";

    // list of templates to split, this should be passed in from the workflow
    @JsonProperty("Templates")
    private List<String> templates;

    @JsonProperty("DiscardFields")
    private List<String> discardFields;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return templates.size();
    }

    public List<String> getTemplates() {
        return templates;
    }

    public void setTemplates(List<String> templates) {
        this.templates = templates;
    }

    public List<String> getDiscardFields() {
        return discardFields;
    }

    public void setDiscardFields(List<String> discardFields) {
        this.discardFields = discardFields;
    }
}

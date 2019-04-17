package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransactionStandardizerConfig extends TransformerConfig {

    @JsonProperty("StringFields")
    private List<String> stringFields;

    @JsonProperty("LongFields")
    private List<String> longFields;

    @JsonProperty("IntFields")
    private List<String> intFields;

    @JsonProperty("CustomField")
    private String customField;

    public List<String> getStringFields() {
        return stringFields;
    }

    public void setStringFields(List<String> stringFields) {
        this.stringFields = stringFields;
    }

    public List<String> getLongFields() {
        return longFields;
    }

    public void setLongFields(List<String> longFields) {
        this.longFields = longFields;
    }

    public List<String> getIntFields() {
        return intFields;
    }

    public void setIntFields(List<String> intFields) {
        this.intFields = intFields;
    }

    public String getCustomField() {
        return customField;
    }

    public void setCustomField(String customField) {
        this.customField = customField;
    }

}

package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import org.codehaus.jackson.annotate.JsonProperty;

public class ContactNameConcatenateConfig extends TransformerConfig {
    @JsonProperty("RetainFields")
    private String[] retainFields;

    @JsonProperty("ConcatenateFields")
    private String[] concatenateFields;

    @JsonProperty("ResultField")
    private String resultField;

    public String[] getRetainFields() {
        return retainFields;
    }

    public void setRetainFields(String[] retainFields) {
        this.retainFields = retainFields;
    }

    public String[] getConcatenateFields() {
        return concatenateFields;
    }

    public void setConcatenateFields(String[] concatenateFields) {
        this.concatenateFields = concatenateFields;
    }

    public String getResultField() {
        return resultField;
    }

    public void setResultField(String resultField) {
        this.resultField = resultField;
    }
}

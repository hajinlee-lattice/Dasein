package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import org.codehaus.jackson.annotate.JsonProperty;

public class ContactNameConcatenateConfig extends TransformerConfig {
    @JsonProperty("ConcatenateFields")
    private String[] concatenateFields;

    @JsonProperty("ResultField")
    private String resultField;

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

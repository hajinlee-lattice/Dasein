package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMDecoderConfig extends TransformerConfig {

    @JsonProperty("DecodeFields")
    private String[] decodeFields;  // attributes to decode

    @JsonProperty("RetainFields")
    private String[] retainFields;  // plain attributes in target source

    public String[] getDecodeFields() {
        return decodeFields;
    }

    public void setDecodeFields(String[] decodeFields) {
        this.decodeFields = decodeFields;
    }

    public String[] getRetainFields() {
        return retainFields;
    }

    public void setRetainFields(String[] retainFields) {
        this.retainFields = retainFields;
    }
}

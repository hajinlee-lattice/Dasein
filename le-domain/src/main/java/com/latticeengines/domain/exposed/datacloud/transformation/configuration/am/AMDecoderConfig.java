package com.latticeengines.domain.exposed.datacloud.transformation.configuration.am;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class AMDecoderConfig extends TransformerConfig {

    @JsonProperty("DecodeFields")
    private String[] decodeFields; // attributes to decode

    // Plain attributes to retain in target source
    // If not provided for non-DecodeAll mode, retain LatticeID, LDC_Domain,
    // LDC_DUNS by default
    // If not provided for DecodeAll mode, retain all plain attributes
    @JsonProperty("RetainFields")
    private String[] retainFields;

    // If DecodeAll is true, will decode all the encoded attributes in AM
    // Please set the flag CAUTIOUSLY. It could generate 7+TB data in hdfs
    @JsonProperty("DecodeAll")
    private boolean decodeAll;

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

    public boolean isDecodeAll() {
        return decodeAll;
    }

    public void setDecodeAll(boolean decodeAll) {
        this.decodeAll = decodeAll;
    }

}

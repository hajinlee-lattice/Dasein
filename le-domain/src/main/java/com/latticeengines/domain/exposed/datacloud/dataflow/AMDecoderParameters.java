package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import java.util.Map;

public class AMDecoderParameters extends TransformationFlowParameters {
    @JsonProperty("DecodeFields")
    private String[] decodeFields;

    @JsonProperty("RetainFields")
    private String[] retainFields;

    @JsonProperty("CodeBookMap")
    private Map<String, BitCodeBook> codeBookMap; // encoded attr -> bitCodeBook

    @JsonProperty("CodeBookLookup")
    private Map<String, String> codeBookLookup; // decoded attr -> encoded attr

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

    public Map<String, BitCodeBook> getCodeBookMap() {
        return codeBookMap;
    }

    public void setCodeBookMap(Map<String, BitCodeBook> codeBookMap) {
        this.codeBookMap = codeBookMap;
    }

    public Map<String, String> getCodeBookLookup() {
        return codeBookLookup;
    }

    public void setCodeBookLookup(Map<String, String> codeBookLookup) {
        this.codeBookLookup = codeBookLookup;
    }
}

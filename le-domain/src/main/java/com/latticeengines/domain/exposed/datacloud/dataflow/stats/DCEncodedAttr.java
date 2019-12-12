package com.latticeengines.domain.exposed.datacloud.dataflow.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DCEncodedAttr implements Serializable {

    private static final long serialVersionUID = -1L;

    @JsonProperty("encoded_attr")
    private String encAttr;

    @JsonProperty("bucketed_attrs")
    private List<DCBucketedAttr> bktAttrs = new ArrayList<>();

    // for jackson
    @SuppressWarnings("unused")
    private DCEncodedAttr() {
    }

    public DCEncodedAttr(String encAttr) {
        this.encAttr = encAttr;
    }

    public String getEncAttr() {
        return encAttr;
    }

    public List<DCBucketedAttr> getBktAttrs() {
        return bktAttrs;
    }

    public void addBktAttr(DCBucketedAttr bktAttr) {
        this.bktAttrs.add(bktAttr);
    }

}

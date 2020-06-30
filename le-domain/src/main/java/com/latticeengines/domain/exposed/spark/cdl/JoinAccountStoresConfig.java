package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JoinAccountStoresConfig extends SparkJobConfig {

    public static final String NAME = "joinAccountStores";

    // when not provided will keep all attributes
    // but drop the duplicated attributes from LatticeAccount side
    @JsonProperty("retainAttrs")
    private List<String> retainAttrs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getRetainAttrs() {
        return retainAttrs;
    }

    public void setRetainAttrs(List<String> retainAttrs) {
        this.retainAttrs = retainAttrs;
    }
}


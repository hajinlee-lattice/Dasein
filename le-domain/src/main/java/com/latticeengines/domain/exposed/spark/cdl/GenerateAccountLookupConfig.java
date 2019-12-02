package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateAccountLookupConfig extends SparkJobConfig {

    public static final String NAME = "generateAccountLookup";

    @JsonProperty("LookupIds")
    private List<String> lookupIds;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getLookupIds() {
        return lookupIds;
    }

    public void setLookupIds(List<String> lookupIds) {
        this.lookupIds = lookupIds;
    }
}

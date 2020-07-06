package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class TruncateLatticeAccountConfig extends SparkJobConfig {

    public static final String NAME = "truncateLatticeAccount";

    // exclude from change list, same as ChangeListConfig.exclusionColumns
    @JsonProperty("ignoreAttrs")
    private List<String> ignoreAttrs;

    // columns to be removed from the base table
    @JsonProperty("removeAttrs")
    private List<String> removeAttrs;

    // in some cases those change list records are generated in enrich step
    @JsonProperty("ignoreRemoveAttrsChangeList")
    private Boolean ignoreRemoveAttrsChangeList;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public List<String> getIgnoreAttrs() {
        return ignoreAttrs;
    }

    public void setIgnoreAttrs(List<String> ignoreAttrs) {
        this.ignoreAttrs = ignoreAttrs;
    }

    public List<String> getRemoveAttrs() {
        return removeAttrs;
    }

    public void setRemoveAttrs(List<String> removeAttrs) {
        this.removeAttrs = removeAttrs;
    }

    public Boolean getIgnoreRemoveAttrsChangeList() {
        return ignoreRemoveAttrsChangeList;
    }

    public void setIgnoreRemoveAttrsChangeList(Boolean ignoreRemoveAttrsChangeList) {
        this.ignoreRemoveAttrsChangeList = ignoreRemoveAttrsChangeList;
    }
}

package com.latticeengines.domain.exposed.spark.common;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ApplyChangeListConfig extends SparkJobConfig {

    public static final String NAME = "applyChangeList";

    @JsonProperty("joinKey")
    private String joinKey;

    // ignore changes to attributes outside of this list
    // this is to enable applying a bigger change list to narrow table
    // it works by excluding invalid "new attributes" when pivoting the change list
    @JsonProperty("includeAttrs")
    private List<String> includeAttrs;

    // if has source table, it is the first input
    @JsonProperty("hasSourceTbl")
    private boolean hasSourceTbl;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public boolean isHasSourceTbl() {
        return hasSourceTbl;
    }

    public void setHasSourceTbl(boolean hasSourceTbl) {
        this.hasSourceTbl = hasSourceTbl;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public List<String> getIncludeAttrs() {
        return includeAttrs;
    }

    public void setIncludeAttrs(List<String> includeAttrs) {
        this.includeAttrs = includeAttrs;
    }
}

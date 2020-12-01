package com.latticeengines.domain.exposed.spark.dcp;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class InputPresenceConfig extends SparkJobConfig {

    public static final String NAME = "InputPresence";

    // these are internal names corresponding to match keys
    @JsonProperty("InputNames")
    private Set<String> inputNames;

    @JsonProperty("ExcludeEmpty")
    private Boolean excludeEmpty;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }

    public Set<String> getInputNames() {
        return inputNames;
    }

    public void setInputNames(Set<String> inputNames) {
        this.inputNames = inputNames;
    }

    public Boolean getExcludeEmpty() {
        return excludeEmpty;
    }

    public void setExcludeEmpty(Boolean excludeEmpty) {
        this.excludeEmpty = excludeEmpty;
    }
}

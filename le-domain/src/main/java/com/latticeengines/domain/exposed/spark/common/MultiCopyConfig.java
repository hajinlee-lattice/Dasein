package com.latticeengines.domain.exposed.spark.common;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MultiCopyConfig extends SparkJobConfig {

    public static final String NAME = "multiCopy";

    @JsonProperty("CopyConfigs")
    private List<CopyConfig> copyConfigs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<CopyConfig> getCopyConfigs() {
        return copyConfigs;
    }

    public void setCopyConfigs(List<CopyConfig> copyConfigs) {
        this.copyConfigs = copyConfigs;
    }

    @Override
    public int getNumTargets() {
        return CollectionUtils.size(copyConfigs);
    }

}

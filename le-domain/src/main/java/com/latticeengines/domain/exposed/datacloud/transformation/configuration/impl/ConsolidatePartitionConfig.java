package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConsolidatePartitionConfig extends TransformerConfig {

    @JsonProperty("name_prefix")
    private String namePrefix;
    private String consolidateDataConfig;
    private String aggregateConfig;
    private String timeField;

    public String getNamePrefix() {
        return this.namePrefix;
    }

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public void setConsolidateDateConfig(String consolidateDataConfig) {
        this.consolidateDataConfig = consolidateDataConfig;
    }

    public String getConsolidateDataConfig() {
        return consolidateDataConfig;
    }

    public String getAggregateConfig() {
        return this.aggregateConfig;
    }

    public void setAggregateConfig(String aggregateConfig) {
        this.aggregateConfig = aggregateConfig;
    }

    public String getTimeField() {
        return this.timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }
}

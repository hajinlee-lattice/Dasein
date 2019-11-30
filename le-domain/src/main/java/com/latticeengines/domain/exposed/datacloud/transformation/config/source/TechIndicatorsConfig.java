package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TechIndicatorsConfig extends TransformerConfig {
    @JsonProperty("GroupByFields")
    private String[] groupByFields;

    @JsonProperty("TimestampField")
    private String timestampField;

    public String[] getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(String[] groupByFields) {
        this.groupByFields = groupByFields;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

}

package com.latticeengines.domain.exposed.datacloud.transformation.config.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

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

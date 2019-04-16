package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MostRecentConfig extends TransformerConfig {
    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("TimestampField")
    private String timestampField;

    @JsonProperty("GroupbyFields")
    private String[] groupbyFields;

    @JsonProperty("PeriodToKeep")
    private Long periodToKeep;

    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public String[] getGroupbyFields() {
        return groupbyFields;
    }

    public void setGroupbyFields(String[] groupbyFields) {
        this.groupbyFields = groupbyFields;
    }

    public Long getPeriodToKeep() {
        return periodToKeep;
    }

    public void setPeriodToKeep(Long periodToKeep) {
        this.periodToKeep = periodToKeep;
    }

}

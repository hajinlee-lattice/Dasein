package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MostRecentParameters extends TransformationFlowParameters {
    @JsonProperty("EarliestToArchive")
    private Date earliestToArchive;

    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("TimestampField")
    private String timestampField;

    @JsonProperty("GroupByFields")
    private String[] groupbyFields;

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

    public Date getEarliest() {
        return earliestToArchive;
    }

    public void setEarliest(Date earliestToArchive) {
        this.earliestToArchive = earliestToArchive;
    }
}

package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MostRecentDataFlowParameters extends DataFlowParameters {

    protected Date earliest;
    private String domainField;
    private String timestampField;
    private String[] groupbyFields;
    private String sourceTable;

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

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public Date getEarliest() {
        return earliest;
    }

    public void setEarliest(Date earliest) {
        this.earliest = earliest;
    }
}

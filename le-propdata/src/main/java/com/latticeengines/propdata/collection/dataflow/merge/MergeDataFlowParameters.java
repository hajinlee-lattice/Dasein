package com.latticeengines.propdata.collection.dataflow.merge;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MergeDataFlowParameters extends DataFlowParameters {

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

    public String getSourceTable() { return sourceTable; }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }
}

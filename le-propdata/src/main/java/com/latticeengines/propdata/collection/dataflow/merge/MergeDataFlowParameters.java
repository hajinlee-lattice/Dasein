package com.latticeengines.propdata.collection.dataflow.merge;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MergeDataFlowParameters extends DataFlowParameters {

    private String domainField;
    private String timestampField;
    private String[] primaryKeys;
    private String[] sourceTables;

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

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String[] primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String[] getSourceTables() { return sourceTables; }

    public void setSourceTables(String[] sourceTables) {
        this.sourceTables = sourceTables;
    }
}

package com.latticeengines.domain.exposed.propdata.dataflow;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CountFlowParameters extends DataFlowParameters {

    private String sourceTable;

    public CountFlowParameters(String sourceTable) {
        setSourceTable(sourceTable);
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }
}

package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class RedshiftDataFlowParameters extends DataFlowParameters {

    @JsonProperty("source_table")
    @SourceTableName
    public String sourceTable;

    public RedshiftDataFlowParameters(String sourceTable) {
        this.sourceTable = sourceTable;
    }
}

package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CreateCdlEventTableFilterParameters extends DataFlowParameters {

    @JsonProperty("train_filter_table")
    @SourceTableName
    public String trainFilterTable;

    @JsonProperty("target_filter_table")
    @SourceTableName
    public String targetFilterTable;

    public CreateCdlEventTableFilterParameters(String trainFilterTable, String targetFilterTable) {
        this.trainFilterTable = trainFilterTable;
        this.targetFilterTable = targetFilterTable;
    }

    public CreateCdlEventTableFilterParameters() {
    }

}

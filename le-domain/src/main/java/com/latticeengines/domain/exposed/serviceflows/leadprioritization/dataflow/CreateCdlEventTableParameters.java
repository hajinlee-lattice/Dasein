package com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CreateCdlEventTableParameters extends DataFlowParameters {

    @JsonProperty("input_table")
    @SourceTableName
    public String inputTable;

    @JsonProperty("aps_table")
    @SourceTableName
    public String apsTable;

    @JsonProperty("account_table")
    @SourceTableName
    public String accountTable;

    public CreateCdlEventTableParameters(String inputTable, String apsTable, String accountTable) {
        this.inputTable = inputTable;
        this.apsTable = apsTable;
        this.accountTable = accountTable;
    }

    public CreateCdlEventTableParameters() {
    }
}

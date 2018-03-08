package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MatchCdlSplitParameters extends DataFlowParameters {

    @JsonProperty("input_table")
    @SourceTableName
    public String inputTable;

    @JsonProperty("expression")
    public String expression;

    @JsonProperty("filter_field")
    public String filterField;

    public MatchCdlSplitParameters() {
    }

    public MatchCdlSplitParameters(String inputTable) {
        this.inputTable = inputTable;
    }

}

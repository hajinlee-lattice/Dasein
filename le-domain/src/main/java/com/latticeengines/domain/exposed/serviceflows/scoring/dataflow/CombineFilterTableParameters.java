package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CombineFilterTableParameters extends DataFlowParameters {

    @JsonProperty("cross_sell_input_table")
    @SourceTableName
    public String crossSellInputTable;

    @JsonProperty("custom_event_input_table")
    @SourceTableName
    public String customEventInputTable;

}

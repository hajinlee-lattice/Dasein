package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.FieldLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.KeyLoadStrategy;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ResolveStagingAndRuntimeTableParameters extends DataFlowParameters {

    @JsonProperty("field_load_strategy")
    public FieldLoadStrategy fieldLoadStrategy;
    
    @JsonProperty("key_load_strategy")
    public KeyLoadStrategy keyLoadStrategy;
    
    @JsonProperty("stage_table")
    public String stageTableName;
    
    @JsonProperty("runtime_table")
    public String runtimeTableName;
    
    @JsonProperty("business_entity")
    public BusinessEntity businessEntity;
}

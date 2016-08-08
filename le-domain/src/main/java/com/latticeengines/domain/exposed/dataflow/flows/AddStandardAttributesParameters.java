package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class AddStandardAttributesParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    @SourceTableName
    public String eventTable;

    @JsonProperty("transform_group")
    public TransformationGroup transformationGroup;
    
    @JsonProperty("do_sort")
    public boolean doSort;

    public AddStandardAttributesParameters(String eventTable, TransformationGroup transformationGroup) {
        this.eventTable = eventTable;
        this.transformationGroup = transformationGroup;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public AddStandardAttributesParameters() {
    }
}

package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class AddStandardAttributesParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    @JsonProperty("transform_group")
    public TransformationGroup transformGroup;

    public AddStandardAttributesParameters(String eventTable, TransformationGroup transformGroup) {
        this.eventTable = eventTable;
        this.transformGroup = transformGroup;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public AddStandardAttributesParameters() {
    }
}

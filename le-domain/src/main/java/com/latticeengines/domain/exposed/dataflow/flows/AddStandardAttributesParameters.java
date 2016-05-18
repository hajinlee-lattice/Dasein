package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class AddStandardAttributesParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    @JsonProperty("transform_group")
    public TransformationGroup transformationGroup;

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

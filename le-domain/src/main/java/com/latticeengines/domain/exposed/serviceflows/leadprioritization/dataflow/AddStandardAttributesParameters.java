package com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

public class AddStandardAttributesParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    @SourceTableName
    public String eventTable;

    @JsonProperty("transforms")
    public List<TransformDefinition> transforms;

    @JsonProperty("do_sort")
    public boolean doSort;

    @JsonProperty("schema_interpretation")
    public SchemaInterpretation sourceSchemaInterpretation;

    public AddStandardAttributesParameters(String eventTable, List<TransformDefinition> transforms, SchemaInterpretation sourceSchemaInterpretation) {
        this.eventTable = eventTable;
        this.transforms = transforms;
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public AddStandardAttributesParameters() {
    }
}

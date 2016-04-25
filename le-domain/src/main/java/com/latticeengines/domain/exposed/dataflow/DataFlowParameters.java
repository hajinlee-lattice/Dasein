package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateScoreTableParameters;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.propdata.match.ParseMatchResultParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = QuotaFlowParameters.class, name = "quotaFlowParameters"), //
        @JsonSubTypes.Type(value = CreateScoreTableParameters.class, name = "createScoreTableParameters"), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreParameters.class, name = "combineInputTableWithScoreParameters"), //
        @JsonSubTypes.Type(value = CreateAttributeLevelSummaryParameters.class, name = "createAttributeLevelSummaryParameters"), //
        @JsonSubTypes.Type(value = DedupEventTableParameters.class, name = "dedupEventTableParameters"), //
        @JsonSubTypes.Type(value = AddStandardAttributesParameters.class, name = "addStandardAttributesParameters"), //
        @JsonSubTypes.Type(value = CreateReportParameters.class, name = "createReportParameters"), //
        @JsonSubTypes.Type(value = ParseMatchResultParameters.class, name = "parseMatchResultParameters"), //
})
public class DataFlowParameters {
}

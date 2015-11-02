package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
@JsonSubTypes.Type(value = QuotaFlowParameters.class, name = "quotaFlowParameters") //
})
public class DataFlowParameters {
}

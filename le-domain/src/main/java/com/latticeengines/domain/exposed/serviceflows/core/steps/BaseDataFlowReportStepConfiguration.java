package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePrematchEventTableReportConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CreatePrematchEventTableReportConfiguration.class, name = "CreatePrematchEventTableReportConfiguration"), })
public class BaseDataFlowReportStepConfiguration extends BaseReportStepConfiguration {
}

package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CreatePreMatchEventTableConfiguration.class, name = "CreatePreMatchEventTableConfiguration"),
        @Type(value = RunAttributeLevelSummaryDataFlowConfiguration.class, name = "RunAttributeLevelSummaryDataFlowConfiguration"),
        @Type(value = RunImportSummaryDataFlowConfiguration.class, name = "RunImportSummaryDataFlowConfiguration"),
        @Type(value = RunScoreTableDataFlowConfiguration.class, name = "RunScoreTableDataFlowConfiguration"), })
public class BasePDDataFlowStepConfiguration extends DataFlowStepConfiguration {

    @Override
    public String getSwlib() {
        return SoftwareLibrary.ProspectDiscovery.getName();
    }

}

package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

public class RunAttributeLevelSummaryDataFlowConfiguration extends BasePDDataFlowStepConfiguration {

    public RunAttributeLevelSummaryDataFlowConfiguration() {
        setBeanName("createAttributeLevelSummary");
        setTargetTableName("CreateAttributeLevelSummary");
    }
}

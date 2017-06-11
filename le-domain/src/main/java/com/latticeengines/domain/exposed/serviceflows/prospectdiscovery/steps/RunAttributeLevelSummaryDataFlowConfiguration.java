package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class RunAttributeLevelSummaryDataFlowConfiguration extends DataFlowStepConfiguration {

    public RunAttributeLevelSummaryDataFlowConfiguration() {
        setBeanName("createAttributeLevelSummary");
        setTargetTableName("CreateAttributeLevelSummary");
    }
}

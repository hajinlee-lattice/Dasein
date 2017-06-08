package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class CreatePreMatchEventTableConfiguration extends DataFlowStepConfiguration {
    public CreatePreMatchEventTableConfiguration() {
        setBeanName("preMatchEventTableFlow");
        setTargetTableName("PrematchFlow");
    }
}

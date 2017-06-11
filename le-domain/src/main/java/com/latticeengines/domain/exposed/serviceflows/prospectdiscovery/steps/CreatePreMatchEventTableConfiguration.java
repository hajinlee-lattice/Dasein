package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class CreatePreMatchEventTableConfiguration extends DataFlowStepConfiguration {
    public CreatePreMatchEventTableConfiguration() {
        setBeanName("preMatchEventTableFlow");
        setTargetTableName("PrematchFlow");
    }
}

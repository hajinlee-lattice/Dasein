package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class CreatePreMatchEventTableConfiguration extends BasePDDataFlowStepConfiguration {
    public CreatePreMatchEventTableConfiguration() {
        setBeanName("preMatchEventTableFlow");
        setTargetTableName("PrematchFlow");
    }
}

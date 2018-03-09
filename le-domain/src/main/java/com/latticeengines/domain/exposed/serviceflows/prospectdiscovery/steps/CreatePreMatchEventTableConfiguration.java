package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

public class CreatePreMatchEventTableConfiguration extends BasePDDataFlowStepConfiguration {
    public CreatePreMatchEventTableConfiguration() {
        setBeanName("preMatchEventTableFlow");
        setTargetTableName("PrematchFlow");
    }
}

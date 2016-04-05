package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class CreatePreMatchEventTableConfiguration extends DataFlowStepConfiguration {
    public CreatePreMatchEventTableConfiguration() {
        setBeanName("preMatchEventTableFlow");
        setTargetTableName("PrematchFlow");
    }
}

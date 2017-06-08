package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class RunImportSummaryDataFlowConfiguration extends DataFlowStepConfiguration {
    public RunImportSummaryDataFlowConfiguration() {
        setBeanName("createImportSummary");
        setTargetTableName("CreateImportSummary");
    }
}

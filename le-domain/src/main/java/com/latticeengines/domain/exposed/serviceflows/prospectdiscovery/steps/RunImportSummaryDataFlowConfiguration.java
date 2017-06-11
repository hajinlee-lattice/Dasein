package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class RunImportSummaryDataFlowConfiguration extends DataFlowStepConfiguration {
    public RunImportSummaryDataFlowConfiguration() {
        setBeanName("createImportSummary");
        setTargetTableName("CreateImportSummary");
    }
}

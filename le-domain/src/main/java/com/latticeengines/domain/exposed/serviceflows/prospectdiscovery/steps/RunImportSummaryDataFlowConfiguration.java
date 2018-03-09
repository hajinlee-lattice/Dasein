package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps;

public class RunImportSummaryDataFlowConfiguration extends BasePDDataFlowStepConfiguration {
    public RunImportSummaryDataFlowConfiguration() {
        setBeanName("createImportSummary");
        setTargetTableName("CreateImportSummary");
    }
}

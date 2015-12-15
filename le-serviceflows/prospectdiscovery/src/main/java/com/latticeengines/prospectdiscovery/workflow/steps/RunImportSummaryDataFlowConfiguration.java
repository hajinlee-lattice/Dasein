package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class RunImportSummaryDataFlowConfiguration extends DataFlowStepConfiguration {
    public RunImportSummaryDataFlowConfiguration() {
        setBeanName("createImportSummary");
        setName("CreateImportSummary");
        setTargetPath("/CreateImportSummary");
    }
}

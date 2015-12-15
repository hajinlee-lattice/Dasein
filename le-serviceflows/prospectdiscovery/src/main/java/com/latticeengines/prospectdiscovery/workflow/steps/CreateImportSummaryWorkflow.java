package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("createImportSummaryWorkflow")
public class CreateImportSummaryWorkflow extends AbstractWorkflow<WorkflowConfiguration> {
    @Autowired
    private RunImportSummaryDataFlow runImportSummaryDataFlow;

    @Autowired
    private RegisterImportSummaryReport registerImportSummaryReport;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(runImportSummaryDataFlow) //
                .next(registerImportSummaryReport) //
                .build();
    }
}

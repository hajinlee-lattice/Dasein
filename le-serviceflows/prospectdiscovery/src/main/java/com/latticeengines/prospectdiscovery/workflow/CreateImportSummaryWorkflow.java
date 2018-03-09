package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.prospectdiscovery.workflow.steps.RegisterImportSummaryReport;
import com.latticeengines.prospectdiscovery.workflow.steps.RunImportSummaryDataFlow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("createImportSummaryWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateImportSummaryWorkflow extends AbstractWorkflow<WorkflowConfiguration> {
    @Autowired
    private RunImportSummaryDataFlow runImportSummaryDataFlow;

    @Autowired
    private RegisterImportSummaryReport registerImportSummaryReport;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(runImportSummaryDataFlow) //
                .next(registerImportSummaryReport) //
                .build();
    }
}

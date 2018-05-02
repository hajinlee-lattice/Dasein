package com.latticeengines.workflowapi.flows.testflows.report;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("testReportWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TestReportWorkflow extends AbstractWorkflow<TestReportWorkflowConfiguration> {
    @Autowired
    private TestRegisterReport registerReport;

    @Override
    public Workflow defineWorkflow(TestReportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(registerReport) //
                .build();
    }
}

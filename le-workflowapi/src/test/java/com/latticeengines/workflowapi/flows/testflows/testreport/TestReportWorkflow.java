package com.latticeengines.workflowapi.flows.testflows.testreport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("testReportWorkflow")
public class TestReportWorkflow extends AbstractWorkflow<TestReportWorkflowConfiguration> {
    @Autowired
    private TestRegisterReport registerReport;

    @Override
    public Workflow defineWorkflow(TestReportWorkflowConfiguration config) {
        return new WorkflowBuilder().next(registerReport).build();
    }
}

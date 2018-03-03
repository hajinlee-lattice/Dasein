package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockCreateAttributeLevelSummaryWorkflow")
public class MockCreateAttributeLevelSummaryWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private RunScoreTableDataFlow runScoreTableDataFlow;

    @Autowired
    private MockRunAttributeLevelSummaryDataFlows mockRunAttributeLevelSummaryDataFlows;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(runScoreTableDataFlow) //
                .next(mockRunAttributeLevelSummaryDataFlows) //
                .next(mockRunAttributeLevelSummaryDataFlows) //
                .build();
    }
}

package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("createAttributeLevelSummaryWorkflow")
public class CreateAttributeLevelSummaryWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private RunScoreTableDataFlow runScoreTableDataFlow;

    @Autowired
    private RunAttributeLevelSummaryDataFlows runAttributeLevelSummaryDataFlows;

    @Bean
    public Job createAttributeLevelSummaryWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(runScoreTableDataFlow) //
                .next(runAttributeLevelSummaryDataFlows) //
                .next(runAttributeLevelSummaryDataFlows) //
                .build();
    }
}

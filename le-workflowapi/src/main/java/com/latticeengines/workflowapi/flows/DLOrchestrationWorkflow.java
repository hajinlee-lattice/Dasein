package com.latticeengines.workflowapi.flows;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelOutputCommandResults;
import com.latticeengines.workflowapi.steps.dlorchestration.RetrieveMetaData;

@Component("dlOrchestrationWorkflow")
public class DLOrchestrationWorkflow extends AbstractWorkflow<DLOrchestrationWorkflowConfiguration> {

    @Autowired
    private RetrieveMetaData retrieveMetaData;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private ModelOutputCommandResults modelOutputCommandResults;

    @Bean
    public Job dLOrchestrationWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(retrieveMetaData) //
                .next(modelWorkflow) //
                .next(modelOutputCommandResults) //
                .build();
    }

}

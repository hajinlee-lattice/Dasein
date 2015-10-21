package com.latticeengines.workflow.library;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractWorkflow;
import com.latticeengines.workflow.build.WorkflowBuilder;
import com.latticeengines.workflow.core.Workflow;
import com.latticeengines.workflow.steps.ModelOutputCommandResults;
import com.latticeengines.workflow.steps.RetrieveMetaData;

@Component("dlOrchestrationWorkflow")
public class DLOrchestrationWorkflow extends AbstractWorkflow {

    @Autowired
    private RetrieveMetaData retrieveMetaData;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private ModelOutputCommandResults modelOutputCommandResults;

    @Bean
    public Job buildDLOrchestrationWorkflow() throws Exception {
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

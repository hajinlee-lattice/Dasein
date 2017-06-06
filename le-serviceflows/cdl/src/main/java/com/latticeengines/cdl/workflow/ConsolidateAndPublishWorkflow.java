package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ConsolidateCompleteListener;
import com.latticeengines.cdl.workflow.steps.ConsolidateData;
import com.latticeengines.cdl.workflow.steps.StartExecution;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("consolidateAndPublishWorkflow")
public class ConsolidateAndPublishWorkflow extends AbstractWorkflow<ConsolidateAndPublishWorkflowConfiguration> {

    @Autowired
    private StartExecution startExecution;

    @Autowired
    private ConsolidateData consolidateData;

    @Autowired
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Autowired
    private ConsolidateCompleteListener consolidateCompleteListener;

    @Bean
    public Job consolidateAndPublishWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(startExecution) //
                .next(consolidateData) //
                .next(redshiftPublishWorkflow) //
                .listener(consolidateCompleteListener) //
                .build();
    }

}

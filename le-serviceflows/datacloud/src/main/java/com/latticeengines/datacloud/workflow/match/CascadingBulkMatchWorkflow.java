package com.latticeengines.datacloud.workflow.match;

import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.CascadingBulkMatchStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cascadingBulkMatchWorkflow")
public class CascadingBulkMatchWorkflow extends AbstractWorkflow<CascadingBulkMatchWorkflowConfiguration> {

    @Autowired
    private CascadingBulkMatchStep cascadingBulkMatchStep;

    @Bean
    public Job cascadingBulkMatchWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cascadingBulkMatchStep) //
                .build();
    }
}

package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePrematchEventTableReport;
import com.latticeengines.leadprioritization.workflow.steps.ValidatePrematchEventTable;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component
public class ModelDataValidationWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private CreatePrematchEventTableReport createPrematchEventTableReport;

    @Autowired
    private ValidatePrematchEventTable validatePrematchEventTable;

    @Bean
    public Job modelDataValidationWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(createPrematchEventTableReport) //
                .next(validatePrematchEventTable) //
                .build();

    }
}

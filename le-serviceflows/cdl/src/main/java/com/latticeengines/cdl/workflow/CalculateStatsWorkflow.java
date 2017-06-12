package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CalculateStatsStep;
import com.latticeengines.cdl.workflow.steps.UpdateStatsObjects;
import com.latticeengines.domain.exposed.serviceflows.cdl.CalculateStatsWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("calculateStatsWorkflow")
public class CalculateStatsWorkflow extends AbstractWorkflow<CalculateStatsWorkflowConfiguration> {

    @Autowired
    private CalculateStatsStep calculateStatsStep;

    @Autowired
    private UpdateStatsObjects updateStatsObjects;

    @Bean
    public Job calculateStatsWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(calculateStatsStep)//
                .next(updateStatsObjects) //
                .build();
    }

}

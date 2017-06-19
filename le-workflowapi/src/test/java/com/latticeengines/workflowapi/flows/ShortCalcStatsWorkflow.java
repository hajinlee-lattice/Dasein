package com.latticeengines.workflowapi.flows;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.CalculateStatsWrapper;
import com.latticeengines.cdl.workflow.steps.UpdateStatsObjects;
import com.latticeengines.domain.exposed.serviceflows.cdl.CalculateStatsWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("shortCalcStatsWorkflow")
public class ShortCalcStatsWorkflow extends AbstractWorkflow<CalculateStatsWorkflowConfiguration> {

    @Autowired
    private CalculateStatsWrapper calculateStatsWrapper;

    @Autowired
    private UpdateStatsObjects updateStatsObjects;

    @Bean
    public Job shortCalcStatsWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(calculateStatsWrapper) //
                .next(updateStatsObjects) //
                .build();
    }

}

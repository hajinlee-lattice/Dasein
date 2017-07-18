package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.CalculateStatsListener;
import com.latticeengines.cdl.workflow.steps.StartProfile;
import com.latticeengines.cdl.workflow.steps.UpdateStatsObjects;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProfileAndPublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("profileAndPublishWorkflow")
public class ProfileAndPublishWorkflow extends AbstractWorkflow<ProfileAndPublishWorkflowConfiguration> {

    @Autowired
    private StartProfile startProfile;

    @Autowired
    private CalculateStatsWrapper calculateStatsWrapper;

    @Autowired
    private SortContactWrapper sortContactWrapper;

    @Autowired
    private UpdateStatsObjects updateStatsObjects;

    @Autowired
    private CalculateStatsListener calculateStatsListener;

    @Autowired
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Bean
    public Job calculateStatsWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(startProfile) //
                .next(calculateStatsWrapper)//
                .next(updateStatsObjects) //
                .next(sortContactWrapper)//
                .next(redshiftPublishWorkflow) //
                .listener(calculateStatsListener) //
                .build();
    }

}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ProfileAndPublishListener;
import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import com.latticeengines.cdl.workflow.steps.FinishProfile;
import com.latticeengines.cdl.workflow.steps.StartProfile;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProfileAndPublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("profileAndPublishWorkflow")
public class ProfileAndPublishWorkflow extends AbstractWorkflow<ProfileAndPublishWorkflowConfiguration> {

    @Inject
    private StartProfile startProfile;

    @Inject
    private CalculateStatsWrapper calculateStatsWrapper;

    @Inject
    private CalculatePurchaseHistoryWrapper calculatePurchaseHistoryWrapper;

    @Inject
    private SortContactWrapper sortContactWrapper;

    @Inject
    private SortProductWrapper sortProductWrapper;

    @Inject
    private CombineStatistics updateStatsObjects;

    @Inject
    private ProfileAndPublishListener profileAndPublishListener;

    @Inject
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Inject
    private FinishProfile finishProfile;

    @Autowired
    private AwsApsGeneratorStep apsGenerator;

    @Bean
    public Job calculateStatsWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(startProfile) //
                .next(calculateStatsWrapper)//
                .next(sortContactWrapper)//
                .next(sortProductWrapper) //
                .next(calculatePurchaseHistoryWrapper)//
                .next(apsGenerator)//
                .next(updateStatsObjects) //
                .next(redshiftPublishWorkflow) //
                .next(finishProfile) //
                .listener(profileAndPublishListener) //
                .build();
    }

}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rating.IngestRuleBasedRating;
import com.latticeengines.domain.exposed.serviceflows.cdl.GenerateRatingWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("generateRatingWorkflow")
@Lazy
public class GenerateRatingWorkflow extends AbstractWorkflow<GenerateRatingWorkflowConfiguration> {

    @Inject
    private GenerateAIRatingWorkflow generateAIRatingWorkflow;

    @Inject
    private IngestRuleBasedRating ingestRatingFromRedshift;

    @Override
    public Workflow defineWorkflow(GenerateRatingWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(generateAIRatingWorkflow, null) //
                .next(ingestRatingFromRedshift) //
                .build();
    }

}

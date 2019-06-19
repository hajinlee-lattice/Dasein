package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rating.ExtractInactiveRatings;
import com.latticeengines.cdl.workflow.steps.rating.ExtractRuleBasedRatings;
import com.latticeengines.cdl.workflow.steps.rating.PivotRatingStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateRatingWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("generateRatingWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateRatingWorkflow extends AbstractWorkflow<GenerateRatingWorkflowConfiguration> {

    @Inject
    private GenerateAIRatingWorkflow generateAIRatingWorkflow;

    @Inject
    private ExtractRuleBasedRatings extractRuleBasedRatings;

    @Inject
    private ExtractInactiveRatings extractInactiveRatings;

    @Inject
    private PivotRatingStep pivotRatingStep;

    @Override
    public Workflow defineWorkflow(GenerateRatingWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(generateAIRatingWorkflow) //
                .next(extractRuleBasedRatings) //
                .next(extractInactiveRatings) //
                .next(pivotRatingStep) //
                .build();
    }

}

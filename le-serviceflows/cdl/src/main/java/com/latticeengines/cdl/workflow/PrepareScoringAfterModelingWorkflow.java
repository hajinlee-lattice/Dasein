package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.inject.Inject;
import com.latticeengines.cdl.workflow.steps.PrepareSegmentMatchingStep;
import com.latticeengines.cdl.workflow.steps.SegmentExportInitStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PrepareScoringAfterModelingWorkflowConfiguration;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("prepareScoringAfterModelingWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareScoringAfterModelingWorkflow
        extends AbstractWorkflow<PrepareScoringAfterModelingWorkflowConfiguration> {

    @Inject
    private SetConfigurationForScoring SetConfigurationForScoring;

    @Inject
    private SegmentExportInitStep segmentExportInitStep;

    @Inject
    private PrepareSegmentMatchingStep prepareSegmentMatchingStep;

    // @Inject
    // private

    @Override
    public Workflow defineWorkflow(PrepareScoringAfterModelingWorkflowConfiguration config) {
        switch (config.getModelingType()) {
        case LPI:
            return new WorkflowBuilder(name()) //
                    .next(SetConfigurationForScoring) //
                    .build();
        case CDL:
        default:
            return new WorkflowBuilder(name()) //
                    .next(segmentExportInitStep) //
                    .next(prepareSegmentMatchingStep) //
                    // .next(matchAccountIdStep) //
                    // .next(matchSplitWithAccountIdStep) //
                    // .next(matchSplitWithoutAccountIdStep) //
                    // .next(matchAccountIdStartStep) //
                    // .next(matchAccountIdWorkflow, null) //
                    // .next(matchWithoutAccountIdStartStep) //
                    // .next(matchWithoutAccountIdWorkflow, null) //
                    // .next(matchMerger) //
                    .build();
        }
    }

}

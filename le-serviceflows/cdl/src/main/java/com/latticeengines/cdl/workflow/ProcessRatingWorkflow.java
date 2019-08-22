package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.rating.CloneInactiveServingStores;
import com.latticeengines.cdl.workflow.steps.rating.PostIterationInitialization;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.cdl.workflow.steps.rating.ProfileRatingWrapper;
import com.latticeengines.cdl.workflow.steps.rating.SplitRatingEngines;
import com.latticeengines.cdl.workflow.steps.rating.StartIteration;
import com.latticeengines.cdl.workflow.steps.reset.ResetRating;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessRatingWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.serviceflows.workflow.export.ExportToRedshift;
import com.latticeengines.serviceflows.workflow.export.ImportGeneratingRatingFromS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processRatingWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessRatingWorkflow extends AbstractWorkflow<ProcessRatingWorkflowConfiguration> {

    @Inject
    private PrepareForRating prepareForRating;

    @Inject
    private SplitRatingEngines splitRatingEngines;

    @Inject
    private ResetRating resetRating;

    @Inject
    private CloneInactiveServingStores cloneInactiveServingStores;

    @Inject
    private StartIteration startIteration;

    @Inject
    private PostIterationInitialization postIterationInitialization;

    @Inject
    private GenerateRatingWorkflow generateRatingWorkflow;

    @Inject
    private ProfileRatingWrapper profileRatingWrapper;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private ExportToRedshift exportToRedshift;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Inject
    private ImportGeneratingRatingFromS3 importGeneratingRatingFromS3;

    @Value("${cdl.pa.default.max.iteration}")
    private int defaultMaxIteration;

    @Override
    public Workflow defineWorkflow(ProcessRatingWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config) //
                .next(prepareForRating) //
                .next(cloneInactiveServingStores) //
                .next(importGeneratingRatingFromS3) //
                .next(splitRatingEngines) //
                .next(resetRating); //
        int maxIteration = config.getMaxIteration() > 0 ? config.getMaxIteration() : defaultMaxIteration;
        for (int i = 0; i < maxIteration; i++) {
            builder = builder.next(startIteration) //
                    .next(postIterationInitialization) //
                    .next(generateRatingWorkflow) //
                    .next(profileRatingWrapper) //
                    .next(combineStatistics) //
                    .next(exportToRedshift) //
                    .next(exportToDynamo);
        }
        return builder.build();
    }
}

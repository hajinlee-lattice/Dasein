package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.rating.CloneInactiveServingStores;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.cdl.workflow.steps.rating.ProfileRatingWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessRatingWorkflowConfiguration;
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
    private CloneInactiveServingStores cloneInactiveServingStores;

    @Inject
    private GenerateRatingWorkflow generateRatingWorkflow;

    @Inject
    private ProfileRatingWrapper profileRatingWrapper;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private ExportDataToRedshift exportDataToRedshift;

    @Override
    public Workflow defineWorkflow(ProcessRatingWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(prepareForRating) //
                .next(cloneInactiveServingStores) //
                .next(generateRatingWorkflow, null) //
                .next(profileRatingWrapper, null) //
                .next(combineStatistics) //
                .next(exportDataToRedshift) //
                .build();
    }
}

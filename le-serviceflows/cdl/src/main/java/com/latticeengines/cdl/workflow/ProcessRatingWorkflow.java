package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.cdl.workflow.steps.rating.ProfileRatingWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processRatingWorkflow")
public class ProcessRatingWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private PrepareForRating prepareForRating;

    @Inject
    private GenerateRatingWorkflow generateRatingWorkflow;

    @Inject
    private ProfileRatingWrapper profileRatingWrapper;

    @Inject
    private ExportDataToRedshift exportDataToRedshift;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(prepareForRating) //
                .next(generateRatingWorkflow) //
                .next(profileRatingWrapper) //
                .next(exportDataToRedshift) //
                .build();
    }
}

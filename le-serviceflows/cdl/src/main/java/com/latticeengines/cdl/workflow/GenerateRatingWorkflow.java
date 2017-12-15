package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rating.IngestRatingFromRedshift;
import com.latticeengines.domain.exposed.serviceflows.cdl.GenerateRatingWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("generateRatingWorkflow")
public class GenerateRatingWorkflow extends AbstractWorkflow<GenerateRatingWorkflowConfiguration> {

    @Inject
    private IngestRatingFromRedshift ingestRatingFromRedshift;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(ingestRatingFromRedshift) //
                .build();
    }

}

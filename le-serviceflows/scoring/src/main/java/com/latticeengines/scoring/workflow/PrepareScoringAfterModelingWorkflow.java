package com.latticeengines.scoring.workflow;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.PrepareScoringAfterModelingWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;

@Component("prepareScoringAfterModelingWorkflow")
@Lazy
public class PrepareScoringAfterModelingWorkflow
        extends AbstractWorkflow<PrepareScoringAfterModelingWorkflowConfiguration> {

    @Override
    public Workflow defineWorkflow(PrepareScoringAfterModelingWorkflowConfiguration workflowConfig) {
        // TODO Auto-generated method stub
        return null;
    }

}

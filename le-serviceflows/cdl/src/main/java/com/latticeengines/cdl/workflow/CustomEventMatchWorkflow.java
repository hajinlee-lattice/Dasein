package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;

@Component("customEventMatchWorkflow")
@Lazy
public class CustomEventMatchWorkflow extends AbstractWorkflow<CustomEventMatchWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Override
    public Workflow defineWorkflow(CustomEventMatchWorkflowConfiguration config) {
        switch (config.getModelingType()) {
        case LPI:
            return matchDataCloudWorkflow.defineWorkflow(null);
        default:
            return null;
        }
    }

}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdFinishStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlAccountWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchCdlWithAccountIdWorkflow")
@Lazy
public class MatchCdlWithAccountIdWorkflow extends AbstractWorkflow<MatchCdlAccountWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private MatchCdlWithAccountIdFinishStep matchAccountIdFinishedStep;

    @Override
    public Workflow defineWorkflow(MatchCdlAccountWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(matchDataCloudWorkflow) //
                .next(matchAccountIdFinishedStep) //
                .build();
    }

}

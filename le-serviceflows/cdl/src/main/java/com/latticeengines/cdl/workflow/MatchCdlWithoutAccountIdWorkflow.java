package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.MatchCdlWithoutAccountIdFinishStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithoutAccountIdStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithoutAccountIdWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchCdlWithoutAccountIdWorkflow")
@Lazy
public class MatchCdlWithoutAccountIdWorkflow extends AbstractWorkflow<MatchCdlWithoutAccountIdWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private MatchCdlWithoutAccountIdStep matchWithoutAccountIdStep;

    @Inject
    private MatchCdlWithoutAccountIdFinishStep matchAccountIdFinishedStep;

    @Override
    public Workflow defineWorkflow(MatchCdlWithoutAccountIdWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(matchDataCloudWorkflow, null) //
                .next(matchWithoutAccountIdStep) //
                .next(matchAccountIdFinishedStep) //
                .build();
    }

}

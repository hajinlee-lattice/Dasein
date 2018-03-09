package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdFinishStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithAccountIdWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchCdlWithAccountIdWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithAccountIdWorkflow extends AbstractWorkflow<MatchCdlWithAccountIdWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private MatchCdlWithAccountIdFinishStep matchAccountIdFinishedStep;

    @Override
    public Workflow defineWorkflow(MatchCdlWithAccountIdWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(matchDataCloudWorkflow, null) //
                .next(matchAccountIdFinishedStep) //
                .build();
    }

}

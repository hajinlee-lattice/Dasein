package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdSimpleFinishStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdSimpleStartStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlAccountWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("customEventSimpleMatchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CustomEventSimpleMatchWorkflow extends AbstractWorkflow<MatchCdlAccountWorkflowConfiguration> {

    @Inject
    private MatchCdlWithAccountIdStep matchAccountIdStep;

    @Inject
    private MatchCdlWithAccountIdSimpleStartStep matchStartStep;

    @Inject
    private MatchCdlWithAccountIdSimpleFinishStep matchFinishStep;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Override
    public Workflow defineWorkflow(MatchCdlAccountWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(matchAccountIdStep) //
                .next(matchStartStep) //
                .next(matchDataCloudWorkflow, null) //
                .next(matchFinishStep) //
                .build();
    }

}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.MatchCdlLdcStartStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlMergeStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlSplitWithAccountIdStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlSplitWithoutAccountIdStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdStartStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithAccountIdStep;
import com.latticeengines.cdl.workflow.steps.MatchCdlWithoutAccountIdStartStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("customEventMatchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CustomEventMatchWorkflow extends AbstractWorkflow<CustomEventMatchWorkflowConfiguration> {

    @Inject
    private MatchCdlLdcStartStep matchCdlLdcStartStep;

    @Inject
    private MatchCdlWithAccountIdStep matchAccountIdStep;

    @Inject
    private MatchCdlSplitWithAccountIdStep matchSplitWithAccountIdStep;

    @Inject
    private MatchCdlSplitWithoutAccountIdStep matchSplitWithoutAccountIdStep;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private MatchCdlWithAccountIdStartStep matchAccountIdStartStep;

    @Inject
    private MatchCdlWithoutAccountIdStartStep matchWithoutAccountIdStartStep;

    @Inject
    private MatchCdlWithAccountIdWorkflow matchAccountIdWorkflow;

    @Inject
    private MatchCdlWithoutAccountIdWorkflow matchWithoutAccountIdWorkflow;

    @Inject
    private MatchCdlMergeStep matchMerger;

    @Override
    public Workflow defineWorkflow(CustomEventMatchWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config);
        switch (config.getModelingType()) {
        case LPI:
            return builder.next(matchCdlLdcStartStep) //
                    .next(matchDataCloudWorkflow) //
                    .build();
        case CDL:
        default:
            return builder.next(matchAccountIdStep) //
                    .next(matchSplitWithAccountIdStep) //
                    .next(matchSplitWithoutAccountIdStep) //
                    .next(matchAccountIdStartStep) //
                    .next(matchAccountIdWorkflow) //
                    .next(matchWithoutAccountIdStartStep) //
                    .next(matchWithoutAccountIdWorkflow) //
                    .next(matchMerger) //
                    .build();
        }
    }

}

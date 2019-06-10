package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.EntityMatchCheckpoint;
import com.latticeengines.cdl.workflow.steps.merge.MatchAccountWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MatchContactWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MatchTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.MatchEntityWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchEntityWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchEntityWorkflow extends AbstractWorkflow<MatchEntityWorkflowConfiguration> {

    @Inject
    private MatchAccountWrapper matchAccountWrapper;

    @Inject
    private MatchContactWrapper matchContactWrapper;

    @Inject
    private MatchTransactionWrapper matchTransactionWrapper;

    @Inject
    private EntityMatchCheckpoint entityMatchCheckpoint;

    @Override
    public Workflow defineWorkflow(MatchEntityWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(matchAccountWrapper) //
                .next(matchContactWrapper) //
                .next(matchTransactionWrapper) //
                .next(entityMatchCheckpoint) //
                .build();
    }
}

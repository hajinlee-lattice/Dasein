package com.latticeengines.datacloud.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.CommitEntityMatch;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CommitEntityMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.CommitEntityMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("commitEntityMatchWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CommitEntityMatchWorkflowImpl extends AbstractWorkflow<CommitEntityMatchWorkflowConfiguration>
        implements CommitEntityMatchWorkflow {

    @Inject
    private CommitEntityMatch commitEntityMatch;

    @Override
    public Workflow defineWorkflow(CommitEntityMatchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(commitEntityMatch) //
                .build();
    }

}

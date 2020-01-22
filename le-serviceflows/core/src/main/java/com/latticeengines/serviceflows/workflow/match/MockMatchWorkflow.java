package com.latticeengines.serviceflows.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockMatchWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MockMatchWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Inject
    private LoadHdfsTableToPDServer loadHdfsTableToPDServer;

    @Inject
    private MockMatch mockMatch;

    @Inject
    private CreateEventTableFromMatchResult createEventTableFromMatchResult;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(loadHdfsTableToPDServer) //
                .next(mockMatch) //
                .next(createEventTableFromMatchResult) //
                .build();
    }
}

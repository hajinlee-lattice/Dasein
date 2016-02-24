package com.latticeengines.serviceflows.workflow.match;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockMatchWorkflow")
public class MockMatchWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private LoadHdfsTableToPDServer loadHdfsTableToPDServer;

    @Autowired
    private MockMatch mockMatch;

    @Autowired
    private CreateEventTableFromMatchResult createEventTableFromMatchResult;

    @Bean
    public Job mockMatchWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(loadHdfsTableToPDServer) //
                .next(mockMatch) //
                .next(createEventTableFromMatchResult) //
                .build();
    }
}

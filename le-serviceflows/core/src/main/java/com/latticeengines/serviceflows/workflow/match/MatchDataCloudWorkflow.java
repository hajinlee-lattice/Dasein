package com.latticeengines.serviceflows.workflow.match;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchDataCloudWorkflow")
public class MatchDataCloudWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private MatchDataCloud matchDataCloud;

    @Autowired
    private ProcessMatchResult processMatchResult;

    @Bean
    public Job matchDataCloudWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(matchDataCloud) //
                .next(processMatchResult) //
                .build();
    }
}

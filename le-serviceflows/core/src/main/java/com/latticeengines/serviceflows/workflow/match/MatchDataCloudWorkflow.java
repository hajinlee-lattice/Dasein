package com.latticeengines.serviceflows.workflow.match;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.datacloud.match.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchDataCloudWorkflow")
public class MatchDataCloudWorkflow extends AbstractWorkflow<MatchDataCloudWorkflowConfiguration> {

    @Autowired
    private PrepareMatchConfig prepareMatchConfig;

    @Autowired
    private BulkMatchWorkflow bulkMatchWorkflow;

    @Autowired
    private ProcessMatchResult processMatchResult;

    @Override
    public Workflow defineWorkflow(MatchDataCloudWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(prepareMatchConfig) //
                .next(bulkMatchWorkflow, null) //
                .next(processMatchResult) //
                .build();
    }
}

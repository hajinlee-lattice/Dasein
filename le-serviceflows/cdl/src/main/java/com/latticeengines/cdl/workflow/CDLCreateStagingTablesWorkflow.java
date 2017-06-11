package com.latticeengines.cdl.workflow;

import com.latticeengines.domain.exposed.serviceflows.cdl.CDLCreateStagingTablesWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.match.MatchListOfEntities;
import com.latticeengines.cdl.workflow.steps.stage.StageListOfEntities;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlCreateStagingTablesWorkflow")
public class CDLCreateStagingTablesWorkflow extends AbstractWorkflow<CDLCreateStagingTablesWorkflowConfiguration> {

    @Autowired
    private StageListOfEntities stageListOfEntities;

    @Autowired
    private MatchListOfEntities matchListOfEntities;

    @Bean
    public Job cdlCreateStagingTablesWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(stageListOfEntities) //
                .next(matchListOfEntities) //
                .build();
    }

}

package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.steps.CreateEventTableReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importMatchAndScoreWorkflow")
public class ImportMatchAndScoreWorkflow extends AbstractWorkflow<ImportMatchAndScoreWorkflowConfiguration> {

    @Autowired
    private ImportData importData;

    @Autowired
    private CreateEventTableReport createEventTableReport;

    @Autowired
    private ScoreWorkflow scoreWorkflow;

    @Bean
    public Job importMatchAndScoreWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(importData) //
                .next(createEventTableReport) //
                .next(scoreWorkflow)//
                .build();
    }

}

package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.importdata.ImportDataFeedTask;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlDataFeedImportWorkflow")
public class CDLDataFeedImportWorkflow extends AbstractWorkflow<CDLDataFeedImportWorkflowConfiguration> {

    @Autowired
    private ImportDataFeedTask importDataFeedTask;

    @Bean
    public Job cdlDataFeedImportWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder()
                .next(importDataFeedTask)
                .build();
    }
}

package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportVdbTableAndPublishWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importvdbtable.ImportVdbTable;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component("importVdbTableAndPublishWorkflow")
public class ImportVdbTableAndPublishWorkflow extends AbstractWorkflow<ImportVdbTableAndPublishWorkflowConfiguration> {

    @Autowired
    private ImportVdbTable importVdbTable;

    @Bean
    public Job importVdbTableAndPublishWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(importVdbTable).build();
    }
}

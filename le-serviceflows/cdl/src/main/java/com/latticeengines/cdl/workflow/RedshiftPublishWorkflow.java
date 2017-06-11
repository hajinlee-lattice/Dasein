package com.latticeengines.cdl.workflow;

import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("redshiftPublishWorkflow")
public class RedshiftPublishWorkflow extends AbstractWorkflow<RedshiftPublishWorkflowConfiguration> {

    @Autowired
    private ExportDataToRedshift exportDataToRedshift;

    @Bean
    public Job redshiftPublishWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(exportDataToRedshift) //
                .build();
    }

}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshiftReportStep;
import com.latticeengines.cdl.workflow.steps.export.PublishToRedshift;
import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishToRedshiftWorkflow")
public class PublishToRedshiftWorkflow extends AbstractWorkflow<RedshiftPublishWorkflowConfiguration> {

    @Inject
    private PublishToRedshift publishToRedshift;

    @Inject
    private ExportDataToRedshiftReportStep exportDataToRedshiftReportStep;

    @Bean
    public Job publishToRedshiftWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(publishToRedshift) //
                .next(exportDataToRedshiftReportStep) //
                .build();
    }

}

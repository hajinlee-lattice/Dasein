package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshiftReportStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("redshiftPublishWorkflow")
@Lazy
public class RedshiftPublishWorkflow extends AbstractWorkflow<RedshiftPublishWorkflowConfiguration> {

    @Autowired
    private ExportDataToRedshift exportDataToRedshift;

    @Autowired
    private ExportDataToRedshiftReportStep exportDataToRedshiftReportStep;

    @Override
    public Workflow defineWorkflow(RedshiftPublishWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(exportDataToRedshift) //
                .next(exportDataToRedshiftReportStep) //
                .build();
    }

}

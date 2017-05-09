package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("redshiftPublishWorkflow")
public class RedshiftPublishWorkflow extends AbstractWorkflow<RedshiftPublishWorkflowConfiguration> {

    @Autowired
    private ExportDataToRedshift exportDataToRedshift;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(exportDataToRedshift) //
                .build();
    }

}

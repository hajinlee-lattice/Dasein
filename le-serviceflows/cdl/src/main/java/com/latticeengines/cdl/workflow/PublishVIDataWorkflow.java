package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.PublishVIDataStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishVIDataWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ImportTableRoleFromS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishVIDataWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishVIDataWorkflow extends AbstractWorkflow<PublishVIDataWorkflowConfiguration> {

    @Inject
    private ImportTableRoleFromS3 importTableRoleFromS3;
    @Inject
    private PublishVIDataStep publishVIDataStep;

    @Override
    public Workflow defineWorkflow(PublishVIDataWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(importTableRoleFromS3)
                .next(publishVIDataStep)
                .build();
    }
}

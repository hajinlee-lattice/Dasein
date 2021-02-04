package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.publish.ImportPublishTableFromS3;
import com.latticeengines.cdl.workflow.steps.publish.PublishTableToElasticSearchStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishTableToElasticSearchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(PublishTableToElasticSearchWorkflowConfiguration.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishTableToElasticSearchWorkflow extends AbstractWorkflow<PublishTableToElasticSearchWorkflowConfiguration> {

    @Inject
    private PublishTableToElasticSearchStep publishTableToElasticSearchStep;

    @Inject
    private ImportPublishTableFromS3 importPublishTableFromS3;

    @Override
    public Workflow defineWorkflow(PublishTableToElasticSearchWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(importPublishTableFromS3)
                .next(publishTableToElasticSearchStep)
                .build();
    }
}

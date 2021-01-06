package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.PublishTableToElasticSearchStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishTableToElasticSearchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishTableToElasticSearchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishTableToElasticSearchWorkflow extends AbstractWorkflow<PublishTableToElasticSearchWorkflowConfiguration> {

    @Inject
    private PublishTableToElasticSearchStep publishTableToElasticSearchStep;

    @Override
    public Workflow defineWorkflow(PublishTableToElasticSearchWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(publishTableToElasticSearchStep)
                .build();
    }
}

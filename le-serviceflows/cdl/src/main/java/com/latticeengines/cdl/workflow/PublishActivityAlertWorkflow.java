package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.PublishActivityAlertWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.PublishActivityAlerts;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(PublishActivityAlertWorkflowConfiguration.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishActivityAlertWorkflow extends AbstractWorkflow<PublishActivityAlertWorkflowConfiguration> {

    @Inject
    private PublishActivityAlerts publishActivityAlerts;

    @Override
    public Workflow defineWorkflow(PublishActivityAlertWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig) //
                .next(publishActivityAlerts).build();
    }
}

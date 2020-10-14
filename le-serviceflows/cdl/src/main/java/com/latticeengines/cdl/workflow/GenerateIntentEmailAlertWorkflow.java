package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import com.latticeengines.cdl.workflow.steps.SendIntentAlertEmailStep;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.process.GenerateIntentAlertArtifacts;
import com.latticeengines.domain.exposed.serviceflows.cdl.GenerateIntentEmailAlertWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(GenerateIntentEmailAlertWorkflowConfiguration.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateIntentEmailAlertWorkflow extends AbstractWorkflow<GenerateIntentEmailAlertWorkflowConfiguration> {

    @Inject
    private GenerateIntentAlertArtifacts generateIntentAlertArtifacts;

    @Inject
    private SendIntentAlertEmailStep sendIntentAlertEmailStep;

    @Override
    public Workflow defineWorkflow(GenerateIntentEmailAlertWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig) //
                .next(generateIntentAlertArtifacts) //
                .next(sendIntentAlertEmailStep) //
                .build();
    }
}

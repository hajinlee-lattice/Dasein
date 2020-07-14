package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.AccountLookupToDynamoStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishAccountLookupWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(PublishAccountLookupWorkflow.NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishAccountLookupWorkflow extends AbstractWorkflow<PublishAccountLookupWorkflowConfiguration> {

    public static final String NAME = "PublishAccountLookupWorkflow";

    @Inject
    private AccountLookupToDynamoStep accountLookupToDynamoStep;

    @Override
    public Workflow defineWorkflow(PublishAccountLookupWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig).next(accountLookupToDynamoStep).build();
    }
}

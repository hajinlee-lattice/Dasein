package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.RegisterImportActionStep;
import com.latticeengines.cdl.workflow.steps.migrate.ContactTemplateMigrateStep;
import com.latticeengines.cdl.workflow.steps.migrate.MigrateContactImportWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ContactImportsMigrateWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("contactImportsMigrateWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ContactImportsMigrateWorkflow extends AbstractWorkflow<ContactImportsMigrateWorkflowConfiguration> {

    @Inject
    private ContactTemplateMigrateStep contactTemplateMigrateStep;

    @Inject
    private MigrateContactImportWrapper migrateContactImportsWrapper;

    @Inject
    private RegisterImportActionStep registerImportActionStep;

    @Override
    public Workflow defineWorkflow(ContactImportsMigrateWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(contactTemplateMigrateStep)
                .next(migrateContactImportsWrapper)
                .next(registerImportActionStep)
                .build();
    }
}

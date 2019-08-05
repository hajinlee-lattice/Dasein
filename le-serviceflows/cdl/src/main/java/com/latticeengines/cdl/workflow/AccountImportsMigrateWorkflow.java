package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.migrate.AccountTemplateMigrateStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.AccountImportsMigrateWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("accountImportsMigrateWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AccountImportsMigrateWorkflow extends AbstractWorkflow<AccountImportsMigrateWorkflowConfiguration> {

    @Inject
    private AccountTemplateMigrateStep accountTemplateMigrateStep;

//    @Inject
//    private MigrateAccountImportsWrapper migrateAccountImportsWrapper;
//
//    @Inject
//    private RegisterImportActionStep registerImportActionStep;

    @Inject
    private ConvertBatchStoreToImportWorkflow convertBatchStoreToImportWorkflow;

    @Override
    public Workflow defineWorkflow(AccountImportsMigrateWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(accountTemplateMigrateStep)
//                .next(migrateAccountImportsWrapper)
//                .next(registerImportActionStep)
                .next(convertBatchStoreToImportWorkflow)
                .build();
    }
}

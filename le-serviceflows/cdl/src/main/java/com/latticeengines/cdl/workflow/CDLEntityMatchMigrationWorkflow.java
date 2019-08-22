package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.CDLEntityMatchMigrationListener;
import com.latticeengines.cdl.workflow.steps.migrate.FinishMigrate;
import com.latticeengines.cdl.workflow.steps.migrate.StartMigrate;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.CDLEntityMatchMigrationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlEntityMatchMigrationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CDLEntityMatchMigrationWorkflow extends AbstractWorkflow<CDLEntityMatchMigrationWorkflowConfiguration> {

    @Inject
    private StartMigrate startMigrate;

    @Inject
    private FinishMigrate finishMigrate;

    @Inject
    private AccountImportsMigrateWorkflow accountImportsMigrateWorkflow;

    @Inject
    private ContactImportsMigrateWorkflow contactImportsMigrateWorkflow;

    @Inject
    private TransactionImportsMigrateWorkflow transactionImportsMigrateWorkflow;

    @Inject
    private CDLEntityMatchMigrationListener cdlEntityMatchMigrationListener;


    @Override
    public Workflow defineWorkflow(CDLEntityMatchMigrationWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(startMigrate)
                .next(accountImportsMigrateWorkflow)
                .next(contactImportsMigrateWorkflow)
                .next(transactionImportsMigrateWorkflow)
                .next(finishMigrate)
                .listener(cdlEntityMatchMigrationListener)
                .build();
    }
}

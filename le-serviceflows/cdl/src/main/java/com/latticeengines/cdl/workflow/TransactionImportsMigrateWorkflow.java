package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.migrate.TransactionTemplateMigrateStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.TransactionImportsMigrateWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("transactionImportsMigrateWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TransactionImportsMigrateWorkflow extends AbstractWorkflow<TransactionImportsMigrateWorkflowConfiguration> {

    @Inject
    private TransactionTemplateMigrateStep transactionTemplateMigrateStep;

    @Inject
    private ConvertBatchStoreToImportWorkflow convertBatchStoreToImportWorkflow;

    @Override
    public Workflow defineWorkflow(TransactionImportsMigrateWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(transactionTemplateMigrateStep)
                .next(convertBatchStoreToImportWorkflow)
                .build();
    }
}

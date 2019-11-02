package com.latticeengines.cdl.workflow.steps.rematch;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.migrate.ConvertBatchStoreToImportWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("convertTransactionWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertTransactionWorkflow extends AbstractWorkflow<ConvertTransactionWorkflowConfiguration> {

    @Inject
    private ConvertBatchStoreToImportWrapper convertBatchStoreToImportWrapper;

    @Inject
    private DeleteByUploadStepWrapper deleteByUploadStepWrapper;

    @Override
    public Workflow defineWorkflow(ConvertTransactionWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(convertBatchStoreToImportWrapper)
                .next(deleteByUploadStepWrapper)
                .build();
    }
}

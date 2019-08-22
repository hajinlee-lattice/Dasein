package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.RegisterImportActionStep;
import com.latticeengines.cdl.workflow.steps.migrate.ConvertBatchStoreToImportWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ConvertBatchStoreToImportWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("convertBatchStoreToImportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToImportWorkflow extends AbstractWorkflow<ConvertBatchStoreToImportWorkflowConfiguration> {

    @Inject
    private ConvertBatchStoreToImportWrapper convertBatchStoreToImportWrapper;

    @Inject
    private RegisterImportActionStep registerImportActionStep;

    @Override
    public Workflow defineWorkflow(ConvertBatchStoreToImportWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(convertBatchStoreToImportWrapper)
                .next(registerImportActionStep)
                .build();
    }
}

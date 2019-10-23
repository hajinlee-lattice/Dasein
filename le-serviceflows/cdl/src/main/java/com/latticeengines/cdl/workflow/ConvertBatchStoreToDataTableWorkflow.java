package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rematch.ConvertAccountWorkflow;
import com.latticeengines.cdl.workflow.steps.rematch.ConvertContactWorkflow;
import com.latticeengines.cdl.workflow.steps.rematch.ConvertTransactionWorkflow;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ConvertBatchStoreToDataTableWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("convertBatchStoreToDataTableWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToDataTableWorkflow extends AbstractWorkflow<ConvertBatchStoreToDataTableWorkflowConfiguration> {

    @Inject
    private ConvertAccountWorkflow convertAccountWorkflow;

    @Inject
    private ConvertContactWorkflow convertContactWorkflow;

    @Inject
    private ConvertTransactionWorkflow convertTransactionWorkflow;

    @Override
    public Workflow defineWorkflow(ConvertBatchStoreToDataTableWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(convertAccountWorkflow)
                .next(convertContactWorkflow)
                .next(convertTransactionWorkflow)
                .build();
    }
}

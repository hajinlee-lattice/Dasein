package com.latticeengines.cdl.workflow.steps.rematch;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.migrate.ConvertBatchStoreToImport;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertAccountWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("convertAccountWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertAccountWorkflow extends AbstractWorkflow<ConvertAccountWorkflowConfiguration> {

    @Inject
    private ConvertBatchStoreToImport convertBatchStoreToImport;

    @Override
    public Workflow defineWorkflow(ConvertAccountWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(convertBatchStoreToImport)
                .build();
    }
}

package com.latticeengines.cdl.workflow.steps.rematch;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertContactWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("convertContactWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertContactWorkflow extends AbstractWorkflow<ConvertContactWorkflowConfiguration> {

    // system batch store is a prerequisite for current rematch PA workflow
    @Inject
    private SplitSystemBatchStore splitSystemBatchStore;

    @Inject
    private DeleteByUploadStepWrapper deleteByUploadStepWrapper;

    @Override
    public Workflow defineWorkflow(ConvertContactWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig).next(splitSystemBatchStore)
                .next(deleteByUploadStepWrapper).build();
    }
}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.legacydelete.LegacyDeleteAccountWorkFlow;
import com.latticeengines.cdl.workflow.steps.legacydelete.LegacyDeleteContactWorkFlow;
import com.latticeengines.cdl.workflow.steps.legacydelete.LegacyDeleteTransactionWorkFlow;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("legacyDeleteWorkFlow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteWorkFlow extends AbstractWorkflow<LegacyDeleteWorkflowConfiguration> {

    @Inject
    private LegacyDeleteAccountWorkFlow legacyDeleteAccountWorkFlow;

    @Inject
    private LegacyDeleteContactWorkFlow legacyDeleteContactWorkFlow;

    @Inject
    private LegacyDeleteTransactionWorkFlow legacyDeleteTransactionWorkFlow;

    @Override
    public Workflow defineWorkflow(LegacyDeleteWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(legacyDeleteAccountWorkFlow)
                .next(legacyDeleteContactWorkFlow)
                .next(legacyDeleteTransactionWorkFlow)
                .build();
    }
}

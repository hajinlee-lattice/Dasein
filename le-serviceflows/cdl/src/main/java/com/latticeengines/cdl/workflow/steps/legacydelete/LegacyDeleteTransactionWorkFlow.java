package com.latticeengines.cdl.workflow.steps.legacydelete;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteTransactionWorkFlowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("legacyDeleteTransactionWorkFlow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteTransactionWorkFlow extends AbstractWorkflow<LegacyDeleteTransactionWorkFlowConfiguration> {
    @Inject
    private LegacyDeleteByUploadStep legacyDeleteByUploadStep;

    @Inject
    private LegacyDeleteByDateRangeStep legacyDeleteByDateRangeStep;

    @Override
    public Workflow defineWorkflow(LegacyDeleteTransactionWorkFlowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(legacyDeleteByUploadStep)
                .next(legacyDeleteByDateRangeStep)
                .build();
    }
}

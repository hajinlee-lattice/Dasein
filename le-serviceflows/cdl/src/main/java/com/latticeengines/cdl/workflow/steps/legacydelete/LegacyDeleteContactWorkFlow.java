package com.latticeengines.cdl.workflow.steps.legacydelete;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteContactWorkFlowConfiguratiion;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("legacyDeleteContactWorkFlow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteContactWorkFlow extends AbstractWorkflow<LegacyDeleteContactWorkFlowConfiguratiion> {

    @Inject
    private LegacyDeleteByUploadWrapper legacyDeleteByUploadWrapper;

    @Override
    public Workflow defineWorkflow(LegacyDeleteContactWorkFlowConfiguratiion workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(legacyDeleteByUploadWrapper)
                .build();
    }
}

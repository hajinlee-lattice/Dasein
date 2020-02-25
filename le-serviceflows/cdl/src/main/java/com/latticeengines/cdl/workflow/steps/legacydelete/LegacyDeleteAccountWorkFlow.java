package com.latticeengines.cdl.workflow.steps.legacydelete;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteAccountWorkFlowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("legacyDeleteAccountWorkFlow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteAccountWorkFlow extends AbstractWorkflow<LegacyDeleteAccountWorkFlowConfiguration> {

    @Inject
    private MergeDeleteStep mergeDeleteStep;

    @Inject
    private LegacyDeleteByUpload legacyDeleteByUpload;

    @Override
    public Workflow defineWorkflow(LegacyDeleteAccountWorkFlowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(mergeDeleteStep)
                .next(legacyDeleteByUpload)
                .build();
    }

}

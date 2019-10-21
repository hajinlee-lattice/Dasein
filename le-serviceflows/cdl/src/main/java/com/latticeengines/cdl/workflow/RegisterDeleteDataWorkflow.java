package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.RegisterDeleteDataListener;
import com.latticeengines.cdl.workflow.steps.maintenance.DeleteFileUploadStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.RegisterDeleteDataWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("registerDeleteDataWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RegisterDeleteDataWorkflow extends AbstractWorkflow<RegisterDeleteDataWorkflowConfiguration> {

    @Inject
    private DeleteFileUploadStep deleteFileUploadStep;

    @Inject
    private RegisterDeleteDataListener registerDeleteDataListener;

    @Override
    public Workflow defineWorkflow(RegisterDeleteDataWorkflowConfiguration workflowConfig) {

        return new WorkflowBuilder(name(), workflowConfig) //
                .next(deleteFileUploadStep) //
                .listener(registerDeleteDataListener) //
                .build();//

    }
}

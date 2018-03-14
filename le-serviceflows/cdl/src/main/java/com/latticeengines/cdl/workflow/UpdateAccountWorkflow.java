package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneAccount;
import com.latticeengines.cdl.workflow.steps.update.ProcessAccountDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateAccountWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateAccountWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountWorkflow extends AbstractWorkflow<UpdateAccountWorkflowConfiguration> {

    @Inject
    private CloneAccount cloneAccount;

    @Inject
    private ProcessAccountDiffWrapper processAccountDiffWrapper;

    @Override
    public Workflow defineWorkflow(UpdateAccountWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cloneAccount) //
                .next(processAccountDiffWrapper) //
                .build();
    }
}

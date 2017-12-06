package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneAccount;
import com.latticeengines.cdl.workflow.steps.update.ProcessAccountDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateAccountWorkflow")
public class UpdateAccountWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private CloneAccount cloneAccount;

    @Inject
    private ProcessAccountDiffWrapper processAccountDiffWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cloneAccount) //
                .next(processAccountDiffWrapper) //
                .build();
    }
}

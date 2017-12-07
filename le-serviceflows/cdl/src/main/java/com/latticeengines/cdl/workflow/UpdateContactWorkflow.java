package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneContact;
import com.latticeengines.cdl.workflow.steps.update.ProcessContactDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateContactWorkflow")
public class UpdateContactWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private CloneContact cloneContact;

    @Inject
    private ProcessContactDiffWrapper processContactDiffWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cloneContact) //
                .next(processContactDiffWrapper) //
                .build();
    }
}

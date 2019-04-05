package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneContact;
import com.latticeengines.cdl.workflow.steps.update.MergeContactDiff;
import com.latticeengines.cdl.workflow.steps.update.ProcessContactDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateContactWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateContactWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateContactWorkflow extends AbstractWorkflow<UpdateContactWorkflowConfiguration> {

    @Inject
    private CloneContact cloneContact;

    @Inject
    private ProcessContactDiffWrapper processContactDiffWrapper;

    @Inject
    private MergeContactDiff mergeContactDiff;

    @Override
    public Workflow defineWorkflow(UpdateContactWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cloneContact) //
                .next(processContactDiffWrapper) //
                .next(mergeContactDiff) //
                .build();
    }
}

package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MergeContactWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetContact;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processContactWorkflow")
@Lazy
public class ProcessContactWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private MergeContactWrapper mergeContactWrapper;

    @Inject
    private UpdateContactWorkflow updateContactWorkflow;

    @Inject
    private RebuildContactWorkflow rebuildContactWorkflow;

    @Inject
    private ResetContact resetContact;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(mergeContactWrapper) //
                .next(updateContactWorkflow) //
                .next(rebuildContactWorkflow) //
                .next(resetContact) //
                .build();
    }
}

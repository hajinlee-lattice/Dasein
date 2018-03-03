package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MergeProductWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetProduct;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processProductWorkflow")
@Lazy
public class ProcessProductWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private MergeProductWrapper mergeProductWrapper;

    @Inject
    private UpdateProductWorkflow updateProductWorkflow;

    @Inject
    private RebuildProductWorkflow rebuildProductWorkflow;

    @Inject
    private ResetProduct resetProduct;

    @Override
    public Workflow defineWorkflow(ProcessAnalyzeWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(mergeProductWrapper) //
                .next(updateProductWorkflow) //
                .next(rebuildProductWorkflow) //
                .next(resetProduct) //
                .build();
    }
}

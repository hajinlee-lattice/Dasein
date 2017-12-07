package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneProduct;
import com.latticeengines.cdl.workflow.steps.update.ProcessProductDiff;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateProductWorkflow")
public class UpdateProductWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private CloneProduct cloneProduct;

    @Inject
    private ProcessProductDiff processProductDiff;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cloneProduct) //
                .next(processProductDiff) //
                .build();
    }
}

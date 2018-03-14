package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.CloneProduct;
import com.latticeengines.cdl.workflow.steps.update.ProcessProductDiff;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateProductWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateProductWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateProductWorkflow extends AbstractWorkflow<UpdateProductWorkflowConfiguration> {

    @Inject
    private CloneProduct cloneProduct;

    @Inject
    private ProcessProductDiff processProductDiff;

    @Override
    public Workflow defineWorkflow(UpdateProductWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cloneProduct) //
                .next(processProductDiff) //
                .build();
    }
}

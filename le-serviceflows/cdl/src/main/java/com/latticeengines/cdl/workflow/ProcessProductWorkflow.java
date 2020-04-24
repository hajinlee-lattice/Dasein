package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MergeProductImportsWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MergeProductSpark;
import com.latticeengines.cdl.workflow.steps.merge.MergeProductWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetProduct;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessProductWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processProductWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessProductWorkflow extends AbstractWorkflow<ProcessProductWorkflowConfiguration> {

    @Inject
    private MergeProductWrapper mergeProductWrapper;

    @Inject
    private MergeProductImportsWrapper mergeProductImportsWrapper;

    @Inject
    private MergeProductSpark mergeProductSpark;

    @Inject
    private UpdateProductWorkflow updateProductWorkflow;

    @Inject
    private RebuildProductWorkflow rebuildProductWorkflow;

    @Inject
    private ResetProduct resetProduct;

    @Value("${cdl.merge.product.use.spark}")
    private boolean useMergeProductSpark;

    @Override
    public Workflow defineWorkflow(ProcessProductWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config) //
                .next(mergeProductImportsWrapper);
        if (useMergeProductSpark) {
            builder = builder.next(mergeProductSpark);
        } else {
            builder = builder.next(mergeProductWrapper);
        }
        return builder //
                .next(updateProductWorkflow) //
                .next(rebuildProductWorkflow) //
                .next(resetProduct) //
                .build();
    }
}

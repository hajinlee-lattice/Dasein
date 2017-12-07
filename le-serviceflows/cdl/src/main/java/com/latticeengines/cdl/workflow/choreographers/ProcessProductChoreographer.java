package com.latticeengines.cdl.workflow.choreographers;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildProductWorkflow;
import com.latticeengines.cdl.workflow.UpdateProductWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeProduct;
import com.latticeengines.cdl.workflow.steps.update.CloneProduct;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessProductChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    @Inject
    private MergeProduct mergeProduct;

    @Inject
    private CloneProduct cloneProduct;

    @Inject
    private UpdateProductWorkflow updateProductWorkflow;

    @Inject
    private RebuildProductWorkflow rebuildProductWorkflow;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep mergeStep() {
        return mergeProduct;
    }

    @Override
    protected AbstractStep cloneStep() {
        return cloneProduct;
    }

    @Override
    protected AbstractWorkflow updateWorkflow() {
        return updateProductWorkflow;
    }

    @Override
    protected AbstractWorkflow rebuildWorkflow() {
        return rebuildProductWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Product;
    }

}

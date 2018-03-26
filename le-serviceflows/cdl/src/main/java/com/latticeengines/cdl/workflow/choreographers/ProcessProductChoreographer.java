package com.latticeengines.cdl.workflow.choreographers;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildProductWorkflow;
import com.latticeengines.cdl.workflow.UpdateProductWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeProduct;
import com.latticeengines.cdl.workflow.steps.reset.ResetProduct;
import com.latticeengines.cdl.workflow.steps.update.CloneProduct;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessProductChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {
    private static final Logger log = LoggerFactory.getLogger(ProcessProductChoreographer.class);

    @Inject
    private MergeProduct mergeProduct;

    @Inject
    private CloneProduct cloneProduct;

    @Inject
    private ResetProduct resetProduct;

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
    protected AbstractStep resetStep() {
        return resetProduct;
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

    @Override
    protected boolean shouldRebuild() {
        if (reset) {
            log.info("Going to reset " + mainEntity() + ", skipping rebuild.");
            return false;
        }
        if (enforceRebuild) {
            log.info("Enforced to rebuild " + mainEntity());
            return true;
        } else if (hasSchemaChange) {
            log.info("Detected schema change in " + mainEntity() + ", going to rebuild.");
            return true;
        } else if (hasImports) {
            log.info("Has product imports always rebuild " + mainEntity());
            return true;
        }
        log.info("No reason to rebuild " + mainEntity());
        return false;
    }
}

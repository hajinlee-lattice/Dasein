package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteAccount;
import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteContact;
import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteTransaction;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.BaseSoftDeleteEntityConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;

@Component
public class DeleteOperationChoreographer extends BaseChoreographer {

    private static final Logger log = LoggerFactory.getLogger(DeleteOperationChoreographer.class);

    @Inject
    private SoftDeleteAccount softDeleteAccount;

    @Inject
    private SoftDeleteContact softDeleteContact;

    @Inject
    private SoftDeleteTransaction softDeleteTransaction;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (!checkShouldSoftDelete(step)) {
            log.info("There is no soft delete action, skip soft delete step!");
            return true;
        } else if (step.name().equalsIgnoreCase(softDeleteAccount.name()) && !hasAccountBatchStore(step)) {
            log.info("There is soft delete action but no Account batch store, skip Account soft delete.");
            return true;
        } else if (step.name().equalsIgnoreCase(softDeleteContact.name()) && !hasContactBatchStore(step)) {
            log.info("There is soft delete action but no Contact batch store, skip Contact soft delete.");
            return true;
        } else if (step.name().equalsIgnoreCase(softDeleteTransaction.name()) && !hasTransactionRawStore(step)) {
            log.info("There is soft delete action but no Transaction raw store, skip Transaction soft delete.");
            return true;
        } else {
            return needReplace(step);
        }
    }

    private boolean checkShouldSoftDelete(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        return grapherContext.isHasSoftDelete();
    }

    private boolean hasAccountBatchStore(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        return grapherContext.isHasAccountBatchStore();
    }

    private boolean hasContactBatchStore(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        return grapherContext.isHasContactBatchStore();
    }

    private boolean hasTransactionRawStore(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        return grapherContext.isHasTransactionRawStore();
    }

    private boolean needReplace(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step.getConfiguration() instanceof BaseSoftDeleteEntityConfiguration) {
            log.info(String.format("Entity %s needs to replace, skip soft delete",
                    ((BaseSoftDeleteEntityConfiguration)step.getConfiguration()).getMainEntity().name()));
            return ((BaseSoftDeleteEntityConfiguration)step.getConfiguration()).isNeedReplace();
        }
        return false;
    }
}

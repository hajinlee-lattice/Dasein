package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;

import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildContactWorkflow;
import com.latticeengines.cdl.workflow.UpdateContactWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeContact;
import com.latticeengines.cdl.workflow.steps.reset.ResetContact;
import com.latticeengines.cdl.workflow.steps.update.CloneContact;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessContactChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessContactChoreographer.class);

    @Inject
    private MergeContact mergeContact;

    @Inject
    private CloneContact cloneContact;

    @Inject
    private ResetContact resetContact;

    @Inject
    private ProcessAccountChoreographer accountChoreographer;

    @Inject
    private UpdateContactWorkflow updateContactWorkflow;

    @Inject
    private RebuildContactWorkflow rebuildContactWorkflow;

    private boolean hasAttrLifeCycleChange = false;
    private boolean hasAccounts = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep<?> mergeStep() {
        return mergeContact;
    }

    @Override
    protected AbstractStep<?> cloneStep() {
        return cloneContact;
    }

    @Override
    protected AbstractStep<?> resetStep() {
        return resetContact;
    }

    @Override
    protected AbstractWorkflow<?> updateWorkflow() {
        return updateContactWorkflow;
    }

    @Override
    protected AbstractWorkflow<?> rebuildWorkflow() {
        return rebuildContactWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    protected void doInitialize(AbstractStep<? extends BaseStepConfiguration> step) {
        super.doInitialize(step);
        checkAttrLifeCycleChange(step);
        hasAccounts = checkHasAccounts(step);
    }

    private void checkAttrLifeCycleChange(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        hasAttrLifeCycleChange = grapherContext.isHasContactAttrLifeCycleChange();
        log.info("Has life cycle change related to Contact attributes.");
    }

    @Override
    protected boolean shouldRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!hasAccounts) {
            log.info("Should not rebuild, since no accounts.");
            return false;
        } else {
            boolean commonRebuild = super.shouldRebuild(step);
            if (!commonRebuild && !reset) {
                if (accountChoreographer.hasNonTrivialChange()) {
                    log.info("Should rebuild, since account has non-trivial change");
                    return true;
                } else if (hasAttrLifeCycleChange) {
                    log.info("Should rebuild, since has attr life cycle change");
                    return true;
                } else if (hasAccounts && !hasActiveServingStore) {
                    log.info("Should rebuild, since has account, and not reset");
                    return true;
                }
            }
            return commonRebuild;
        }
    }

    @Override
    protected Set<String> getExtraDecisions() {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(hasAttrLifeCycleChange ? "hasAttrLifeCycleChange=true" : "");
        decisions.add(accountChoreographer.hasNonTrivialChange() ? "hasNonTrivialChange=true" : "");
        decisions.add(hasAccounts && !hasActiveServingStore ? "hasNoActiveServingStore=true" : "");
        return decisions;
    }

    @Override
    protected boolean shouldUpdate() {
        if (!hasAccounts) {
            log.info("Should not update, since no accounts.");
            return false;
        } else {
            return super.shouldUpdate();
        }
    }

    @Override
    protected boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return false;
    }

}

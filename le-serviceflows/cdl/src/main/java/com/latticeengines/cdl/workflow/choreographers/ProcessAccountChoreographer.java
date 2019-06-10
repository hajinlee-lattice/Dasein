package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE;

import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildAccountWorkflow;
import com.latticeengines.cdl.workflow.UpdateAccountWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeAccount;
import com.latticeengines.cdl.workflow.steps.merge.RematchAccount;
import com.latticeengines.cdl.workflow.steps.reset.ResetAccount;
import com.latticeengines.cdl.workflow.steps.update.CloneAccount;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessAccountChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountChoreographer.class);

    @Inject
    private MergeAccount mergeAccount;

    @Inject
    private RematchAccount rematchAccount;

    @Inject
    private CloneAccount cloneAccount;

    @Inject
    private ResetAccount resetAccount;

    @Inject
    private UpdateAccountWorkflow updateAccountWorkflow;

    @Inject
    private RebuildAccountWorkflow rebuildAccountWorkflow;

    protected boolean rebuildNotForDataCloudChange = false;
    protected boolean dataCloudChanged = false;
    private boolean hasAttrLifeCycleChange = false;
    private boolean shouldRematch = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (isRematchStep(step)) {
            return !shouldRematch;
        } else {
            return isCommonSkip(step, seq);
        }
    }

    @Override
    protected AbstractStep<?> mergeStep() {
        return mergeAccount;
    }

    @Override
    protected AbstractStep<?> cloneStep() {
        return cloneAccount;
    }

    @Override
    protected AbstractStep<?> resetStep() {
        return resetAccount;
    }

    @Override
    protected AbstractWorkflow<?> updateWorkflow() {
        return updateAccountWorkflow;
    }

    @Override
    protected AbstractWorkflow<?> rebuildWorkflow() {
        return rebuildAccountWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected void doInitialize(AbstractStep<? extends BaseStepConfiguration> step) {
        checkShouldRematch(step);
        super.doInitialize(step);
        checkDataCloudChange(step);
        checkAttrLifeCycleChange(step);
    }

    void checkDataCloudChange(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        dataCloudChanged = grapherContext.isDataCloudChanged();
        log.info("Data cloud verision changed=" + dataCloudChanged + " for " + mainEntity());
    }

    private void checkAttrLifeCycleChange(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        hasAttrLifeCycleChange = grapherContext.isHasAccountAttrLifeCycleChange();
        log.info("Has life cycle change related to Account attributes.");
    }

    private void checkShouldRematch(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        shouldRematch = grapherContext.isFullRematch();
    }

    @Override
    protected boolean shouldMerge(AbstractStep<? extends BaseStepConfiguration> step) {
        return super.shouldMerge(step) || hasEmbeddedAccounts(step);
    }

    @Override
    protected boolean shouldRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        rebuildNotForDataCloudChange = super.shouldRebuild(step);
        if (!rebuildNotForDataCloudChange) {
            if (shouldRematch) {
                log.info("Should rebuild, because fully re-matched");
                rebuildNotForDataCloudChange = true;
            } else if (hasAttrLifeCycleChange && !reset) {
                log.info("Should rebuild, because detected attr life cycle change.");
                rebuildNotForDataCloudChange = true;
            } else if (hasEmbeddedAccounts(step) && !reset) {
                log.info("Should rebuild, because detected embedded accounts from other entity");
                rebuildNotForDataCloudChange = true;
            }
        }
        if (!rebuildNotForDataCloudChange && (dataCloudChanged && !reset)) {
            log.info("Should rebuild, because there were data cloud changes.");
            return true;
        }
        return rebuildNotForDataCloudChange;
    }

    @Override
    protected boolean shouldReset(AbstractStep<? extends BaseStepConfiguration> step) {
        if (hasEmbeddedAccounts(step)) {
            return false;
        }
        return super.shouldReset(step);
    }

    @Override
    protected Set<String> getExtraDecisions() {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(dataCloudChanged ? "dataCloudChanged=true" : "");
        decisions.add(hasAttrLifeCycleChange ? "hasAttrLifeCycleChange=true" : "");
        decisions.add(shouldRematch ? "shouldRematch=true" : "");
        return decisions;
    }

    @Override
    protected boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        Set<BusinessEntity> entities = step.getSetObjectFromContext(BaseWorkflowStep.PA_SKIP_ENTITIES,
                BusinessEntity.class);
        boolean skip = CollectionUtils.isNotEmpty(entities) && entities.contains(mainEntity());
        if (skip) {
            AbstractWorkflow<?> workflow = rebuildWorkflow();
            String namespace = getStepNamespace(seq);
            return workflow.name().equals(namespace) || namespace.startsWith(workflow.name() + ".")
                    || namespace.contains("." + workflow.name() + ".") || namespace.endsWith("." + workflow.name());

        }
        return false;
    }

    private boolean isRematchStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(rematchAccount.name());
    }

    boolean hasNonTrivialChange() {
        return rebuild || update;
    }

    /*
     * check whether we have embedded accounts created by matching other entities
     */
    private boolean hasEmbeddedAccounts(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step == null) {
            return false;
        }
        return StringUtils.isNotBlank(step.getStringValueFromContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE))
                || StringUtils.isNotBlank(step.getStringValueFromContext(ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE));
    }

    // For quicker testing purpose on local: skip account rebuild in txn end2end
    // test with entity match enabled -- Replace hasEmbeddedAccounts() with
    // hasEmbeddedAccountsForRebuild() in shouldRebuild()
    private boolean hasEmbeddedAccountsForRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step == null) {
            return false;
        }
        return StringUtils.isNotBlank(step.getStringValueFromContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE));
    }
}

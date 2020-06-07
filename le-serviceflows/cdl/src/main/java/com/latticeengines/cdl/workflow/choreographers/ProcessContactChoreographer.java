package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.RebuildContactWorkflow;
import com.latticeengines.cdl.workflow.UpdateContactWorkflow;
import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteContact;
import com.latticeengines.cdl.workflow.steps.merge.MergeContact;
import com.latticeengines.cdl.workflow.steps.reset.ResetContact;
import com.latticeengines.cdl.workflow.steps.update.CloneContact;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessContactChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessContactChoreographer.class);

    @Inject
    private SoftDeleteContact softDeleteContact;

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

    @Inject
    private BatonService batonService;

    private boolean hasAttrLifeCycleChange = false;
    private boolean hasAccounts = false;
    private boolean hasNonTrivialAccountChange = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep<?> softDeleteStep() {
        return softDeleteContact;
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
        if (!hasAccounts && !replace && !shouldSoftDelete(step)) {
            log.info("Should not rebuild, since no accounts and no need to replace.");
            return false;
        } else {
            boolean shouldRebuild;
            ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                    ChoreographerContext.class);
            if (grapherContext.isAlwaysRebuildServingStores()) {
                shouldRebuild = true;
            } else {
                shouldRebuild = super.shouldRebuild(step);
                if (!shouldRebuild && !reset) {
                    boolean attHotFix = false;
                    if (step.getConfiguration() instanceof BaseProcessEntityStepConfiguration) {
                        BaseProcessEntityStepConfiguration stepConfig = (BaseProcessEntityStepConfiguration) step.getConfiguration();
                        String tenantId = stepConfig.getCustomerSpace().getTenantId();
                        attHotFix = batonService.shouldSkipFuzzyMatchInPA(tenantId);
                        if (attHotFix) {
                            log.info("ATT hotfix, ignoring non-trivial account change.");
                        }
                    }
                    if (accountChoreographer.hasDelete()) {
                        log.info("Should rebuild, since account has delete");
                        shouldRebuild = true;
                    } else if (accountChoreographer.hasNonTrivialChange() && !attHotFix) {
                        log.info("Should rebuild, since account has non-trivial change");
                        shouldRebuild = true;
                    } else if (hasAttrLifeCycleChange) {
                        log.info("Should rebuild, since has attr life cycle change");
                        shouldRebuild = true;
                    } else if (hasAccounts && !hasActiveServingStore) {
                        log.info("Should rebuild, since has account, and not reset");
                        shouldRebuild = true;
                    }
                }
            }
            return shouldRebuild;
        }
    }

    @Override
    protected Set<String> getExtraDecisions() {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(hasAttrLifeCycleChange ? "hasAttrLifeCycleChange=true" : "");
        decisions.add(hasNonTrivialAccountChange ? "hasNonTrivialChange=true" : "");
        decisions.add(hasAccounts && !hasActiveServingStore ? "hasNoActiveServingStore=true" : "");
        return decisions;
    }

    @Override
    protected boolean shouldUpdate(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!hasAccounts) {
            log.info("Should not update, since no accounts.");
            return false;
        } else {
            return super.shouldUpdate(step);
        }
    }

    @Override
    protected boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return false;
    }

    @Override
    boolean hasValidSoftDeleteActions(List<Action> softDeletes) {
        return CollectionUtils.isNotEmpty(softDeletes) && softDeletes.stream().anyMatch(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            return configuration.hasEntity(BusinessEntity.Contact);
        });
    }

    @Override
    protected boolean hasEmbeddedEntity(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step == null) {
            return false;
        }

        boolean hasInStream = hasTableInMapCtx(step, ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE);
        log.info("Found embedded account from activity stream: {}", hasInStream);
        return hasInStream;
    }
}

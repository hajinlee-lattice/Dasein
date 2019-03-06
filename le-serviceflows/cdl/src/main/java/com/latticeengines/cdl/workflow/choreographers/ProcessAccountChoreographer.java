package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;

import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildAccountWorkflow;
import com.latticeengines.cdl.workflow.UpdateAccountWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeAccount;
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

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
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

    @Override
    protected boolean shouldRebuild() {
        rebuildNotForDataCloudChange = super.shouldRebuild();
        rebuildNotForDataCloudChange = rebuildNotForDataCloudChange || (hasAttrLifeCycleChange && !reset);
        return rebuildNotForDataCloudChange || (dataCloudChanged && !reset);
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

}

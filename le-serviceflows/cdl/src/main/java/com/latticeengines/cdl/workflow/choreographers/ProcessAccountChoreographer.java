package com.latticeengines.cdl.workflow.choreographers;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildAccountWorkflow;
import com.latticeengines.cdl.workflow.UpdateAccountWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeAccount;
import com.latticeengines.cdl.workflow.steps.reset.ResetAccount;
import com.latticeengines.cdl.workflow.steps.update.CloneAccount;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessAccountChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

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

    protected boolean commonRebuild = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep mergeStep() {
        return mergeAccount;
    }

    @Override
    protected AbstractStep cloneStep() {
        return cloneAccount;
    }

    @Override
    protected AbstractStep resetStep() {
        return resetAccount;
    }

    @Override
    protected AbstractWorkflow updateWorkflow() {
        return updateAccountWorkflow;
    }

    @Override
    protected AbstractWorkflow rebuildWorkflow() {
        return rebuildAccountWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected boolean shouldRebuild() {
        commonRebuild = super.shouldRebuild();
        return commonRebuild || dataCloudChanged;
    }

}

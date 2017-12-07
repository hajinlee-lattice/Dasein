package com.latticeengines.cdl.workflow.choreographers;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildContactWorkflow;
import com.latticeengines.cdl.workflow.UpdateContactWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeContact;
import com.latticeengines.cdl.workflow.steps.update.CloneContact;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessContactChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    @Inject
    private MergeContact mergeContact;

    @Inject
    private CloneContact cloneContact;

    @Inject
    private UpdateContactWorkflow updateContactWorkflow;

    @Inject
    private RebuildContactWorkflow rebuildContactWorkflow;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep mergeStep() {
        return mergeContact;
    }

    @Override
    protected AbstractStep cloneStep() {
        return cloneContact;
    }

    @Override
    protected AbstractWorkflow updateWorkflow() {
        return updateContactWorkflow;
    }

    @Override
    protected AbstractWorkflow rebuildWorkflow() {
        return rebuildContactWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Contact;
    }

}

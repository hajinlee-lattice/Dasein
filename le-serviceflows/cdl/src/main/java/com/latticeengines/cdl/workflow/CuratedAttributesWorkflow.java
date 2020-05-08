package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.CuratedAccountAttributesWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.CuratedContactAttributes;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileCuratedAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileCuratedContactWrapper;
import com.latticeengines.cdl.workflow.steps.update.CloneCuratedAccountAttributes;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.CuratedAttributesWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("curatedAttributesWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedAttributesWorkflow extends AbstractWorkflow<CuratedAttributesWorkflowConfiguration> {

    @Inject
    private CloneCuratedAccountAttributes cloneCuratedAccountAttributes;

    @Inject
    private CuratedAccountAttributesWrapper curatedAccountAttributesWrapper;

    @Inject
    private CuratedContactAttributes curatedContactAttributes;

    @Inject
    private ProfileCuratedAccountWrapper profileCuratedAccount;

    @Inject
    private ProfileCuratedContactWrapper profileCuratedContact;

    @Override
    public Workflow defineWorkflow(CuratedAttributesWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cloneCuratedAccountAttributes) //
                .next(curatedAccountAttributesWrapper) //
                .next(curatedContactAttributes) //
                .next(profileCuratedAccount) //
                .next(profileCuratedContact) //
                .build();
    }
}

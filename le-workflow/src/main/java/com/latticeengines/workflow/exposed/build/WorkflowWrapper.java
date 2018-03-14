package com.latticeengines.workflow.exposed.build;

import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public abstract class WorkflowWrapper<S extends BaseWrapperStepConfiguration, W extends WorkflowConfiguration, R extends BaseWrapperStep<S, W>, I extends WorkflowInterface<W>>
        extends AbstractWorkflow<W> {

    @Override
    public Workflow defineWorkflow(W config) {
        return new WorkflowBuilder(null, config) //
                .next(getWrapperStep()) //
                .next(getWrappedWorkflow()) //
                .next(getWrapperStep()) //
                .build();
    }

    protected abstract R getWrapperStep();

    protected abstract I getWrappedWorkflow();

}

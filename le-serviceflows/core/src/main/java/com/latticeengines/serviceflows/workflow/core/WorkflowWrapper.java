package com.latticeengines.serviceflows.workflow.core;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflow.exposed.build.WorkflowInterface;

public abstract class WorkflowWrapper<W extends BaseWrapperStep, C extends WorkflowInterface>
        extends AbstractWorkflow<TransformationConfiguration> {

    @SuppressWarnings("unchecked")
    @Override
    public Workflow defineWorkflow(TransformationConfiguration config) {
        return new WorkflowBuilder() //
                .next(getWrapperStep()) //
                .next(getWrappedWorkflow()) //
                .next(getWrapperStep()) //
                .build();
    }

    protected abstract W getWrapperStep();

    protected abstract C getWrappedWorkflow();

}

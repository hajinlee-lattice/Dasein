package com.latticeengines.serviceflows.workflow.core;

import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflow.exposed.build.WorkflowInterface;

public abstract class WorkflowWrapper<W extends BaseWrapperStep, C extends WorkflowInterface>
        extends AbstractWorkflow<TransformationConfiguration> {

    @Bean
    public Job matchDataCloudWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(getWrapperStep()) //
                .next(getWrappedWorkflow()) //
                .next(getWrapperStep()) //
                .build();
    }

    protected abstract W getWrapperStep();

    protected abstract C getWrappedWorkflow();

}

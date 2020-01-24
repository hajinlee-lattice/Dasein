package com.latticeengines.datacloud.workflow.engine;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.Publish;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.PublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PublishWorkflow extends AbstractWorkflow<PublishWorkflowConfiguration> {

    @Inject
    private Publish publish;

    @Override
    public Workflow defineWorkflow(PublishWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(publish) //
                .build();
    }

}

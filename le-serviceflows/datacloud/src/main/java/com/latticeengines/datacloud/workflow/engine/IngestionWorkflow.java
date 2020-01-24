package com.latticeengines.datacloud.workflow.engine;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.IngestionStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.IngestionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ingestionWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class IngestionWorkflow extends AbstractWorkflow<IngestionWorkflowConfiguration> {
    @Inject
    private IngestionStep ingestionStep;

    @Override
    public Workflow defineWorkflow(IngestionWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(ingestionStep) //
                .build();
    }

}

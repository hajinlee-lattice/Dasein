package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.BuildCatalogWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessCatalogWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(ProcessCatalogWorkflow.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessCatalogWorkflow extends AbstractWorkflow<ProcessCatalogWorkflowConfiguration> {

    static final String WORKFLOW_NAME = "processCatalogWorkflow";

    @Inject
    private BuildCatalogWrapper buildCatalogWrapper;

    @Override
    public Workflow defineWorkflow(ProcessCatalogWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(buildCatalogWrapper) //
                .build();
    }
}

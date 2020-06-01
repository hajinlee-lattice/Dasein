package com.latticeengines.cdl.workflow;


import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.PublishTableRoleToDynamo;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishDynamoWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishDynamoWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishDynamoWorkflow extends AbstractWorkflow<PublishDynamoWorkflowConfiguration>  {

    @Inject
    private PublishTableRoleToDynamo publish;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Override
    public Workflow defineWorkflow(PublishDynamoWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config).next(publish).next(exportToDynamo).build();
    }

}

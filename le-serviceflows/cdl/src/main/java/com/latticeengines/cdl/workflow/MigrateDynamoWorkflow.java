package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.importdata.ImportDynamoTableFromS3;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.MigrateDynamoWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("migrateDynamoWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateDynamoWorkflow extends AbstractWorkflow<MigrateDynamoWorkflowConfiguration> {

    @Inject
    private ImportDynamoTableFromS3 importDynamoTableFromS3;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Override
    public Workflow defineWorkflow(MigrateDynamoWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config).next(importDynamoTableFromS3).next(exportToDynamo).build();
    }
}

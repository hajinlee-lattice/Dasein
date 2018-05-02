package com.latticeengines.workflowapi.flows.testflows.dynamo;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("testDynamoWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TestDynamoWorkflow extends AbstractWorkflow<TestDynamoWorkflowConfiguration> {

    @Inject
    private PrepareTestDynamo prepare;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Override
    public Workflow defineWorkflow(TestDynamoWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepare) //
                .next(exportToDynamo) //
                .build();
    }
}

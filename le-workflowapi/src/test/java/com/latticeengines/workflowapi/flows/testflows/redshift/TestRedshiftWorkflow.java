package com.latticeengines.workflowapi.flows.testflows.redshift;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.ExportToRedshift;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("testRedshiftWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TestRedshiftWorkflow extends AbstractWorkflow<TestRedshiftWorkflowConfiguration> {

    @Inject
    private PrepareTestRedshift prepare;

    @Inject
    private ExportToRedshift export;

    @Override
    public Workflow defineWorkflow(TestRedshiftWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepare) //
                .next(export) //
                .build();
    }
}

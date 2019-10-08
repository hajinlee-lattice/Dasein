package com.latticeengines.cdl.workflow;


import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.FinishMockActivityStore;
import com.latticeengines.cdl.workflow.steps.MockActivityStore;
import com.latticeengines.domain.exposed.serviceflows.cdl.MockActivityStoreWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.serviceflows.workflow.export.ExportToRedshift;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockActivityStoreWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MockActivityStoreWorkflow extends AbstractWorkflow<MockActivityStoreWorkflowConfiguration> {

    @Inject
    private MockActivityStore mock;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Inject
    private ExportToRedshift exportToRedshift;

    @Inject
    private FinishMockActivityStore finish;

    @Override
    public Workflow defineWorkflow(MockActivityStoreWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(mock) //
                .next(exportToRedshift) //
                .next(exportToDynamo) //
                .next(finish) //
                .build();

    }

}

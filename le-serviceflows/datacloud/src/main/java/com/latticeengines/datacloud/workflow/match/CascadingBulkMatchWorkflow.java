package com.latticeengines.datacloud.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.CascadingBulkMatchStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cascadingBulkMatchWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CascadingBulkMatchWorkflow extends AbstractWorkflow<CascadingBulkMatchWorkflowConfiguration> {

    @Inject
    private CascadingBulkMatchStep cascadingBulkMatchStep;

    @Override
    public Workflow defineWorkflow(CascadingBulkMatchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cascadingBulkMatchStep) //
                .build();
    }
}

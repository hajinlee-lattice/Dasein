package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.EnrichWebVisitWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateVisitReportWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(GenerateVisitReportWorkflow.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateVisitReportWorkflow extends AbstractWorkflow<GenerateVisitReportWorkflowConfiguration> {

    static final String WORKFLOW_NAME = "generateVisitReportWorkflow";

    @Inject
    private EnrichWebVisitWrapper enrichWebVisitWrapper;

    @Override
    public Workflow defineWorkflow(GenerateVisitReportWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig) //
                .next(enrichWebVisitWrapper)
                .build();
    }
}

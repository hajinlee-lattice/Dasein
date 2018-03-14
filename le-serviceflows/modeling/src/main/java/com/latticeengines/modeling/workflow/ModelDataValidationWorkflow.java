package com.latticeengines.modeling.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.modeling.ModelDataValidationWorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.CreatePrematchEventTableReport;
import com.latticeengines.modeling.workflow.steps.ValidatePrematchEventTable;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelDataValidationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ModelDataValidationWorkflow extends AbstractWorkflow<ModelDataValidationWorkflowConfiguration> {

    @Inject
    private CreatePrematchEventTableReport createPrematchEventTableReport;

    @Inject
    private ValidatePrematchEventTable validatePrematchEventTable;

    @Override
    public Workflow defineWorkflow(ModelDataValidationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(createPrematchEventTableReport) //
                .next(validatePrematchEventTable) //
                .build();

    }
}

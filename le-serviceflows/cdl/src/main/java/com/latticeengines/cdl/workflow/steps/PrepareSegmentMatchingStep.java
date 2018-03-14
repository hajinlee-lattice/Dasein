package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("prepareSegmentMatchingStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareSegmentMatchingStep extends BaseWorkflowStep<MicroserviceStepConfiguration> {

    @Override
    public void execute() {
        // TODO Auto-generated method stub

    }

}

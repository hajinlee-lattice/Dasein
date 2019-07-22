package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("registerImportActionStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RegisterImportActionStep extends BaseWorkflowStep<RegisterImportActionStepConfiguration> {
    @Override
    public void execute() {

    }
}

package com.latticeengines.cdl.workflow.steps.reset;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ResetAccount extends BaseResetEntityStep<ProcessAccountStepConfiguration> {
}

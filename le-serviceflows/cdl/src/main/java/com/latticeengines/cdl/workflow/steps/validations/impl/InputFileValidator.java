package com.latticeengines.cdl.workflow.steps.validations.impl;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.validations.BaseInputFileValidator;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;

@Component("inputFileValidator")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class InputFileValidator extends BaseInputFileValidator<InputFileValidatorConfiguration> {

    @Override
    protected BusinessEntity getEntity() {
        return configuration.getEntity();
    }
}

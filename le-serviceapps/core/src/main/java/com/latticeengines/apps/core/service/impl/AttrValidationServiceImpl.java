package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;

@Component("attrValidationService")
public class AttrValidationServiceImpl implements AttrValidationService {

    private List<String> validatorList = new ArrayList<>();

    public void setValidateParam(List<AttrConfig> existingConfigs) {
        LimitationValidator limitationValidator = (LimitationValidator) AttrValidator
                .getValidator(LimitationValidator.VALIDATOR_NAME);
        limitationValidator.setDBConfigs(existingConfigs);
        UsageLimitValidator usageLimitationValidator = (UsageLimitValidator) AttrValidator
                .getValidator(UsageLimitValidator.VALIDATOR_NAME);
        usageLimitationValidator.setDBConfigs(existingConfigs);
    }

    @PostConstruct
    private void initializeValidator() {
        validatorList.add(CDLImpactValidator.VALIDATOR_NAME);
        validatorList.add(GenericValidator.VALIDATOR_NAME);
        validatorList.add(LifecycleValidator.VALIDATOR_NAME);
        validatorList.add(LimitationValidator.VALIDATOR_NAME);
        validatorList.add(UsageValidator.VALIDATOR_NAME);
        validatorList.add(UsageLimitValidator.VALIDATOR_NAME);
    }

    @Override
    public ValidationDetails validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        for (String validatorName : validatorList) {
            AttrValidator validator = AttrValidator.getValidator(validatorName);
            if (validator != null) {
                validator.validate(attrConfigs, isAdmin);
            }
        }
        return generateReport(attrConfigs);
    }

    private ValidationDetails generateReport(List<AttrConfig> attrConfigs) {
        ValidationDetails details = new ValidationDetails();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            for (AttrConfig attrConfig : attrConfigs) {
                if (attrConfig.getImpactWarnings() != null || attrConfig.getValidationErrors() != null) {
                    ValidationDetails.AttrValidation validation = new ValidationDetails.AttrValidation();
                    validation.setAttrName(attrConfig.getAttrName());
                    validation.setImpactWarnings(attrConfig.getImpactWarnings());
                    validation.setValidationErrors(attrConfig.getValidationErrors());
                    details.addValidation(validation);
                }
            }
            String msg = String.format("GenerateReport %d attr configs", attrConfigs.size());
            timer.setTimerMessage(msg);
        }
        return details;
    }
}

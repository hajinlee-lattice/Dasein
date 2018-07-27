package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;

@Component("attrValidationService")
public class AttrValidationServiceImpl implements AttrValidationService {

    private List<String> activationValidatorList = new ArrayList<>();
    private List<String> usageValidatorList = new ArrayList<>();

    @PostConstruct
    private void initializeValidator() {
        activationValidatorList.add(GenericValidator.VALIDATOR_NAME);
        activationValidatorList.add(LifecycleValidator.VALIDATOR_NAME);
        activationValidatorList.add(ActivationLimitValidator.VALIDATOR_NAME);
        activationValidatorList.add(UsageValidator.VALIDATOR_NAME);

        usageValidatorList.add(CDLImpactValidator.VALIDATOR_NAME);
        usageValidatorList.add(GenericValidator.VALIDATOR_NAME);
        usageValidatorList.add(UsageLimitValidator.VALIDATOR_NAME);
    }

    @Override
    public ValidationDetails validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrConfigUpdateMode mode) {
        List<String> validatorList = activationValidatorList;
        if (AttrConfigUpdateMode.Usage.equals(mode)) {
            validatorList = usageValidatorList;
        }
        for (String validatorName : validatorList) {
            AttrValidator validator = AttrValidator.getValidator(validatorName);
            try (PerformanceTimer timer = new PerformanceTimer()) {
                if (validator != null) {
                    validator.validate(existingAttrConfigs, userProvidedAttrConfigs);
                }
                String msg = String.format("Validator %s for tenant %s", validatorName,
                        MultiTenantContext.getShortTenantId());
                timer.setTimerMessage(msg);
            }
        }
        return generateReport(userProvidedAttrConfigs);
    }

    private ValidationDetails generateReport(List<AttrConfig> attrConfigs) {
        ValidationDetails details = new ValidationDetails();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            for (AttrConfig attrConfig : attrConfigs) {
                if (attrConfig.getImpactWarnings() != null || attrConfig.getValidationErrors() != null) {
                    ValidationDetails.AttrValidation validation = new ValidationDetails.AttrValidation();
                    validation.setAttrName(attrConfig.getAttrName());
                    validation
                            .setCategory(attrConfig.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class));
                    validation.setSubcategory(
                            attrConfig.getPropertyFinalValue(ColumnMetadataKey.Subcategory, String.class));
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

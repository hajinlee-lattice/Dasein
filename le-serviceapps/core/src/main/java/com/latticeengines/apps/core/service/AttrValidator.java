package com.latticeengines.apps.core.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public abstract class AttrValidator {

    private static final Map<String, AttrValidator> validators = new HashMap<>();

    protected AttrValidator(String name) {
        validators.put(name, this);
    }

    public static AttrValidator getValidator(String name) {
        return validators.get(name);
    }

    public abstract void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrValidation validation);

    public void addErrorMsg(ValidationErrors.Type errorType, String message, AttrConfig attrConfig) {
        ValidationErrors error = attrConfig.getValidationErrors();
        if (error == null) {
            error = new ValidationErrors();
            error.getErrors().put(errorType, new ArrayList<>());
            error.getErrors().get(errorType).add(message);
            attrConfig.setValidationErrors(error);
        } else {
            if (error.getErrors().containsKey(errorType)) {
                if (!error.getErrors().get(errorType).contains(message)) {
                    error.getErrors().get(errorType).add(message);
                }
            } else {
                error.getErrors().put(errorType, new ArrayList<>());
                error.getErrors().get(errorType).add(message);
            }
        }
    }

    public void addWarningMsg(ImpactWarnings.Type warningType, String message, AttrConfig attrConfig) {
        ImpactWarnings warning = attrConfig.getImpactWarnings();
        if (warning == null) {
            warning = new ImpactWarnings();
            warning.getWarnings().put(warningType, new ArrayList<>());
            warning.getWarnings().get(warningType).add(message);
            attrConfig.setImpactWarnings(warning);
        } else {
            if (warning.getWarnings().containsKey(warningType)) {
                if (!warning.getWarnings().get(warningType).contains(message)) {
                    warning.getWarnings().get(warningType).add(message);
                }
            } else {
                warning.getWarnings().put(warningType, new ArrayList<>());
                warning.getWarnings().get(warningType).add(message);
            }
        }
    }
}

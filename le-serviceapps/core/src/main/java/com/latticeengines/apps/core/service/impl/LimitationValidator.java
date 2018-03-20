package com.latticeengines.apps.core.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

@Component("limitationValidator")
public class LimitationValidator extends AttrValidator {

    public static final String VALIDATOR_NAME = "LIMITATION_VALIDATOR";

    protected LimitationValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs) {

    }
}

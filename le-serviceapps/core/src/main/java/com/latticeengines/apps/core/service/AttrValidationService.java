package com.latticeengines.apps.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;

public interface AttrValidationService {

    ValidationDetails validate(List<AttrConfig> attrConfigs, boolean isAdmin);
}

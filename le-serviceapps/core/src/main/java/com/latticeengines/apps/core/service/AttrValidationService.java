package com.latticeengines.apps.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;

public interface AttrValidationService {

    ValidationDetails validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
            AttrConfigUpdateMode mode);

}

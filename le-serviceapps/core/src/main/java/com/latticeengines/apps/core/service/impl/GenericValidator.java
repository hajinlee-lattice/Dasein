package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("genericValidator")
public class GenericValidator extends AttrValidator {

    public static final String VALIDATOR_NAME = "GENERIC_VALIDATOR";

    protected GenericValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        for (AttrConfig attrConfig : attrConfigs) {
            checkInvalidPropChange(attrConfig, isAdmin);
        }
    }


    private void checkInvalidPropChange(AttrConfig attrConfig, boolean isAdmin) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        for (Map.Entry<String, AttrConfigProp<?>> attrProp : attrConfigPropMap.entrySet()) {
            if (!isAdmin && !attrProp.getValue().isAllowCustomization()) {
                if(attrProp.getValue().getCustomValue() != null) {
                    addErrorMsg(ValidationErrors.Type.INVALID_PROP_CHANGE,
                            String.format(ValidationMsg.Errors.FORBID_CUSTOMIZATION, attrProp.getKey()),
                            attrConfig);
                }
            }
        }
    }

}

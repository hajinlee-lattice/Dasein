package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("lifecycleValidator")
public class LifecycleValidator extends AttrValidator {

    public static final String VALIDATOR_NAME = "LIFECYCLE_VALIDATOR";

    protected LifecycleValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs) {
        for (AttrConfig attrConfig : attrConfigs) {
            checkState(attrConfig);
        }
    }

    private void checkState(AttrConfig attrConfig) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp stateProp = attrConfig.getProperty(ColumnMetadataKey.State);
        if(stateProp != null) {
            if(stateProp.getCustomValue() != null) {
                AttrState customState = AttrState.valueOf(stateProp.getCustomValue().toString());
                if (customState.equals(AttrState.Inactive)) {
                    addErrorMsg(ValidationErrors.Type.INVALID_ACTIVATION,
                            String.format(ValidationMsg.Errors.FORBID_SET_INACTIVE, attrConfig.getAttrName()),
                            attrConfig);
                }
            }
            if (stateProp.getSystemValue() != null) {
                AttrState systemState = AttrState.valueOf(stateProp.getSystemValue().toString());
                if (systemState.equals(AttrState.Inactive)) {
                    for (ColumnSelection.Predefined group: ColumnSelection.Predefined.values()) {
                        AttrConfigProp groupUsageProp = attrConfig.getProperty(group.name());
                        if (groupUsageProp != null) {
                            if (groupUsageProp.getCustomValue() != null) {
                                addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                        String.format(ValidationMsg.Errors.UPDATE_INACTIVE,
                                                group.name(), attrConfig.getAttrName()),
                                        attrConfig);
                            }
                            if (groupUsageProp.getSystemValue() != null) {
                                Boolean groupUsage = Boolean.valueOf(groupUsageProp.getSystemValue().toString());
                                if (groupUsage) {
                                    addErrorMsg(ValidationErrors.Type.INVALID_USAGE_CHANGE,
                                            String.format(ValidationMsg.Errors.INACTIVE_USAGE,
                                                    attrConfig.getAttrName(), group.name()),
                                            attrConfig);
                                }
                            }
                        }
                    }
                } else if (systemState.equals(AttrState.Deprecated)) {
                    if (stateProp.getCustomValue() != null) {
                        AttrState customState = AttrState.valueOf(stateProp.getCustomValue().toString());
                        if (customState.equals(AttrState.Active)) {
                            addErrorMsg(ValidationErrors.Type.INVALID_ACTIVATION,
                                    String.format(ValidationMsg.Errors.FORBID_SET_ACTIVE, attrConfig.getAttrName()),
                                    attrConfig);
                        }
                    }
                }
            }
        }


    }
}

package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("lifecycleValidator")
public class LifecycleValidator extends AttrValidator {

    private static Logger log = LoggerFactory.getLogger(LifecycleValidator.class);
    public static final String VALIDATOR_NAME = "LIFECYCLE_VALIDATOR";

    protected LifecycleValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs) {
        log.info("start to validate lifecycle");
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkState(attrConfig);
        }
    }

    private void checkState(AttrConfig attrConfig) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp<?> stateProp = attrConfig.getProperty(ColumnMetadataKey.State);
        AttrState finalState = attrConfig.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
        if (stateProp != null) {
            if (stateProp.getCustomValue() != null) {
                AttrState customState = AttrState.valueOf(stateProp.getCustomValue().toString());
                // do not allow activation of deprecated attributes
                if (customState.equals(AttrState.Active)) {
                    AttrState systemState = AttrState.valueOf(stateProp.getSystemValue().toString());
                    if (systemState.equals(AttrState.Deprecated)) {
                        addErrorMsg(ValidationErrors.Type.INVALID_ACTIVATION,
                                String.format(ValidationMsg.Errors.FORBID_SET_ACTIVE, attrConfig.getAttrName()),
                                attrConfig);
                    }
                }
            }
            // check customer value or system value is equal to Inactive
            if (AttrState.Inactive.equals(finalState)) {
                for (ColumnSelection.Predefined group : ColumnSelection.Predefined.usageProperties) {
                    Boolean finalUsageValue = attrConfig.getPropertyFinalValue(group.name(), Boolean.class);
                    if (Boolean.TRUE.equals((finalUsageValue))) {
                        addWarningMsg(ImpactWarnings.Type.USAGE_ENABLED, group.name(), attrConfig);
                    }
                }
            }
        }
    }
}

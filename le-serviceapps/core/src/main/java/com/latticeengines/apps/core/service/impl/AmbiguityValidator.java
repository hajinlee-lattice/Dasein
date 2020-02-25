package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;

@Component("ambiguityValidator")
public class AmbiguityValidator extends AttrValidator {

    private static final Logger log = LoggerFactory.getLogger(AmbiguityValidator.class);
    public static final String VALIDATOR_NAME = "AMBIGUITY_VALIDATOR";

    protected AmbiguityValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> existingAttrConfigs, List<AttrConfig> userProvidedAttrConfigs,
                         ValidationDetails.AttrValidation validation) {
        log.info(String.format("start to validate name for tenant %s", MultiTenantContext.getShortTenantId()));
        Set<String> displayNames =
                existingAttrConfigs.stream().map(config -> config.getPropertyFinalValue(ColumnMetadataKey.DisplayName,
                        String.class)).filter(Objects::nonNull).map(String::toLowerCase).collect(Collectors.toSet());
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkDuplicatedName(attrConfig, displayNames);
        }
    }

    private void checkDuplicatedName(AttrConfig attrConfig, Set<String> attrNames) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp<?> displayNameGroup = attrConfig.getProperty(ColumnMetadataKey.DisplayName);
        if (displayNameGroup != null && displayNameGroup.getCustomValue() != null) {
            String displayName = displayNameGroup.getCustomValue().toString().toLowerCase();
            if (attrNames.contains(displayName)) {
                addErrorMsg(ValidationErrors.Type.DUPLICATE_NAME_CHANGE,
                        ValidationMsg.Errors.DUPLICATED_NAME, attrConfig);
            } else {
                attrNames.add(displayName);
            }
        }
    }

}

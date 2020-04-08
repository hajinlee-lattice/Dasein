package com.latticeengines.apps.core.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        // (attrName -> dispName.lower)
        Map<AttributeLookup, String> attrToDispMap = new HashMap<>();
        Map<AttributeLookup, AttrConfig> configMap = new HashMap<>();
        existingAttrConfigs.forEach(config -> {
            String attrName = config.getAttrName();
            BusinessEntity entity = BusinessEntity.getCentralEntity(config.getEntity());
            AttributeLookup attr = new AttributeLookup(entity, attrName);
            if (configMap.containsKey(attr)) {
                // not necessary, just as a debug log
                log.warn("Duplicated config: {} -- {}", JsonUtils.serialize(configMap.get(attr)), JsonUtils.serialize(config));
            }
            configMap.put(attr, config);
            String dispName = config.getPropertyFinalValue(ColumnMetadataKey.DisplayName,
                    String.class);
            if (StringUtils.isNotBlank(dispName)) {
                attrToDispMap.put(attr, dispName.toLowerCase());
            }
        });
        // apply this in-coming change to existing disp-name customizations
        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            updateDispToAttrMap(attrConfig, attrToDispMap);
        }

        Map<String, Integer> accountDispNameOccurrence = countDispNameOccurrence(attrToDispMap, BusinessEntity.Account);
        Map<String, Integer> contactDispNameOccurrence = countDispNameOccurrence(attrToDispMap, BusinessEntity.Contact);

        for (AttrConfig attrConfig : userProvidedAttrConfigs) {
            checkDuplicatedName(attrConfig, accountDispNameOccurrence, contactDispNameOccurrence);
        }
    }

    // update the mapping based on current request
    private void updateDispToAttrMap(AttrConfig attrConfig, Map<AttributeLookup, String> attrToDispMap) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp<?> displayNameGroup = attrConfig.getProperty(ColumnMetadataKey.DisplayName);
        if (displayNameGroup != null && displayNameGroup.getCustomValue() != null) {
            String displayNameLower = displayNameGroup.getCustomValue().toString().toLowerCase();
            String attrName = attrConfig.getAttrName();
            BusinessEntity entity = BusinessEntity.getCentralEntity(attrConfig.getEntity());
            AttributeLookup attr = new AttributeLookup(entity, attrName);
            attrToDispMap.put(attr, displayNameLower);
        }
    }

    private Map<String, Integer> countDispNameOccurrence(Map<AttributeLookup, String> attrToDispMap, BusinessEntity entity) {
        Map<String, Integer> dispNameOccurrence = new HashMap<>();
        for (Map.Entry<AttributeLookup, String> entry: attrToDispMap.entrySet()) {
            if (entity.equals(entry.getKey().getEntity())) {
                String dispNameLower = entry.getValue();
                int count = dispNameOccurrence.getOrDefault(dispNameLower, 0);
                dispNameOccurrence.put(dispNameLower, count + 1);
            }
        }
        return dispNameOccurrence;
    }

    private void checkDuplicatedName(AttrConfig attrConfig, Map<String, Integer> accountDispNameOccurrence,
                                     Map<String, Integer> contactDispNameOccurrence) {
        Map<String, AttrConfigProp<?>> attrConfigPropMap = attrConfig.getAttrProps();
        if (MapUtils.isEmpty(attrConfigPropMap)) {
            return;
        }
        AttrConfigProp<?> displayNameGroup = attrConfig.getProperty(ColumnMetadataKey.DisplayName);
        if (displayNameGroup != null && displayNameGroup.getCustomValue() != null) {
            String displayNameLower = displayNameGroup.getCustomValue().toString().toLowerCase();
            int occurrence = BusinessEntity.Account.equals(attrConfig.getEntity())
                    ? accountDispNameOccurrence.get(displayNameLower)
                    : contactDispNameOccurrence.get(displayNameLower);
            if (occurrence > 1) {
                addErrorMsg(ValidationErrors.Type.DUPLICATE_NAME_CHANGE,
                        ValidationMsg.Errors.DUPLICATED_NAME, attrConfig);
            }
        }
    }

}

package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;

public final class LimitValidatorUtils {

    protected LimitValidatorUtils() {
        throw new UnsupportedOperationException();
    }

    public static void checkAmbiguityInFieldNames(List<AttrConfig> attrConfigs) {
        Set<String> attrSet = new HashSet<>();
        if (!CollectionUtils.isEmpty(attrConfigs)) {
            for (AttrConfig config : attrConfigs) {
                String attrName = config.getAttrName();
                if (attrSet.contains(attrName)) {
                    throw new LedpException(LedpCode.LEDP_18113, new String[] { attrName });
                }
                attrSet.add(attrName);
            }
        }

    }

    public static <T extends Serializable> List<AttrConfig> returnPropertyConfigs(List<AttrConfig> attrConfigs,
            String propName, T value) {
        List<AttrConfig> stateConfigs = new ArrayList<>();
        for (AttrConfig config : attrConfigs) {
            try {
                if (config.getAttrProps().get(propName) != null
                        && value.equals(getActualValue(config.getAttrProps().get(propName)))) {
                    stateConfigs.add(config);
                }
            } catch (NullPointerException e) {
                throw new LedpException(LedpCode.LEDP_40026, new String[] { config.getAttrName() });
            }
        }
        return stateConfigs;
    }

    static <T extends Serializable> Object getActualValue(AttrConfigProp<T> configProp) {
        if (configProp.isAllowCustomization()) {
            if (configProp.getCustomValue() != null) {
                return configProp.getCustomValue();
            }
        }
        return configProp.getSystemValue();
    }

    public static List<AttrConfig> generateUnionConfig(List<AttrConfig> existingActiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        List<AttrConfig> result = new ArrayList<AttrConfig>();
        result.addAll(userSelectedActiveConfigs);
        Set<String> attrNames = userSelectedActiveConfigs.stream().map(config -> config.getAttrName())
                .collect(Collectors.toSet());
        existingActiveConfigs.forEach(config -> {
            if (!attrNames.contains(config.getAttrName())) {
                result.add(config);
            }
        });
        return result;
    }

    public static List<AttrConfig> generateInterceptionConfig(List<AttrConfig> existingConfigs,
            List<AttrConfig> userConfigs) {
        List<AttrConfig> result = new ArrayList<AttrConfig>();
        Set<String> attrNames = existingConfigs.stream().map(config -> config.getAttrName())
                .collect(Collectors.toSet());
        userConfigs.forEach(config -> {
            if (attrNames.contains(config.getAttrName())) {
                result.add(config);
            }
        });
        return result;
    }

}

package com.latticeengines.domain.exposed.encryption;

import org.apache.commons.lang3.BooleanUtils;

import com.latticeengines.common.exposed.util.PropertyUtils;

public class EncryptionGlobalState {
    public static boolean isEnabled() {
        boolean property = Boolean.parseBoolean(PropertyUtils.getProperty("encryption.enabled"));
        boolean environment = BooleanUtils.toBoolean(System.getenv("LE_ENCRYPTION_DISABLED"));
        return property && !environment;
    }
}

package com.latticeengines.domain.exposed.util;

import javax.persistence.AttributeConverter;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.CipherUtils;

public class ColumnEncryptorDecryptor implements AttributeConverter<String, String> {
    @Override
    public String convertToDatabaseColumn(String unencrypted) {
        if (StringUtils.isBlank(unencrypted))
            return unencrypted;
        return CipherUtils.encrypt(unencrypted);
    }

    @Override
    public String convertToEntityAttribute(String encrypted) {
        if (StringUtils.isBlank(encrypted))
            return encrypted;
        return CipherUtils.decrypt(encrypted);
    }
}

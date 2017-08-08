package com.latticeengines.oauth2.authserver;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.OAuth2RefreshToken;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;

public class JsonJdbcTokenStore extends JdbcTokenStore {

    JsonJdbcTokenStore(DataSource dataSource) {
        super(dataSource);
    }

    protected byte[] serializeAccessToken(OAuth2AccessToken token) {
        return serialize(token);
    }

    protected byte[] serializeRefreshToken(OAuth2RefreshToken token) {
        return serialize(token);
    }

    protected byte[] serializeAuthentication(OAuth2Authentication authentication) {
        return serialize(authentication);
    }

    protected OAuth2AccessToken deserializeAccessToken(byte[] token) {
        return deserialize(token, OAuth2AccessToken.class);
    }

    protected OAuth2RefreshToken deserializeRefreshToken(byte[] token) {
        return deserialize(token, OAuth2RefreshToken.class);
    }

    protected OAuth2Authentication deserializeAuthentication(byte[] authentication) {
        return deserialize(authentication, OAuth2Authentication.class);
    }

    private byte[] serialize(Object token) {
        byte[] bytes = JsonUtils.serialize(token).getBytes(Charset.forName("UTF-8"));
        try {
            return CompressionUtils.compressByteArray(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress byte array.", e);
        }
    }

    private <T> T deserialize(byte[] bytes, Class<T> clz) {
        String uncompressedData = new String(CompressionUtils.decompressByteArray(bytes), Charset.forName("UTF-8"));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            return JsonUtils.deserialize(uncompressedData, clz);
        } else {
            return null;
        }
    }

}

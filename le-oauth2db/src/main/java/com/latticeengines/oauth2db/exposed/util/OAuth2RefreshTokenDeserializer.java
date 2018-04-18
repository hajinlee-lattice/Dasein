package com.latticeengines.oauth2db.exposed.util;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.common.DefaultExpiringOAuth2RefreshToken;
import org.springframework.security.oauth2.common.DefaultOAuth2RefreshToken;
import org.springframework.security.oauth2.common.OAuth2RefreshToken;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuth2RefreshTokenDeserializer extends JsonDeserializer<OAuth2RefreshToken> {

    private static final Logger log = LoggerFactory.getLogger(OAuth2RefreshTokenDeserializer.class);

    private static final String STRING_VALUE = "value";
    private static final String LONG_EXPIRATION = "expiration";

    @Override
    public OAuth2RefreshToken deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {

        ObjectCodec oc = jp.getCodec();
        JsonNode node = oc.readTree(jp);

        return convertToRefreshToken(jp, node);
    }

    private OAuth2RefreshToken convertToRefreshToken(JsonParser jp, JsonNode refreshTokenNode) {

        if (refreshTokenNode == null || refreshTokenNode.getNodeType() == JsonNodeType.NULL
                || StringUtils.isBlank(refreshTokenNode.toString())) {
            return null;
        }

        OAuth2RefreshToken refreshToken = null;
        try {
            ObjectNode node = (ObjectNode) refreshTokenNode;
            String tokenValue = getText(node, STRING_VALUE);
            if (StringUtils.isBlank(tokenValue)) {
                return null;
            }

            Long expirationInMSecs = getLong(node, LONG_EXPIRATION);
            if (expirationInMSecs != null) {
                Date expirationDt = new Date(expirationInMSecs);
                refreshToken = new DefaultExpiringOAuth2RefreshToken(tokenValue, expirationDt);
            } else {
                refreshToken = new DefaultOAuth2RefreshToken(tokenValue);
            }
        } catch (Exception e) {
            log.error("Error while deserializing OAuthRefreshToken.", e);
        }
        return refreshToken;
    }

    private String getText(ObjectNode node, String fieldName) {
        JsonNode textNode = node.get(fieldName);
        return textNode == null || textNode.getNodeType() == JsonNodeType.NULL
                || StringUtils.isBlank(textNode.toString()) ? null : textNode.asText();
    }

    private Long getLong(ObjectNode node, String fieldName) {
        JsonNode textNode = node.get(fieldName);
        return textNode == null || textNode.getNodeType() == JsonNodeType.NULL
                || StringUtils.isBlank(textNode.toString()) ? null : textNode.asLong();
    }

}

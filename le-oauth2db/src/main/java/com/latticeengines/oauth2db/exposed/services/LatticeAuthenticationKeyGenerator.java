package com.latticeengines.oauth2db.exposed.services;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.common.util.OAuth2Utils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.AuthenticationKeyGenerator;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLConstants;

@Component
public class LatticeAuthenticationKeyGenerator implements AuthenticationKeyGenerator {

    private static final Logger log = LoggerFactory.getLogger(LatticeAuthenticationKeyGenerator.class);

    private static final String SCOPE = "scope";

    private static final String USERNAME = "username";

    public String extractKey(OAuth2Authentication authentication) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        OAuth2Request authorizationRequest = authentication.getOAuth2Request();
        if (!authentication.isClientOnly()) {
            values.put(USERNAME, authentication.getName());
        }
        values.put(CDLConstants.AUTH_CLIENT_ID, authorizationRequest.getClientId());
        if (authorizationRequest.getScope() != null) {
            values.put(SCOPE, OAuth2Utils.formatParameterList(authorizationRequest.getScope()));
        }

        if (authorizationRequest.getRequestParameters() != null) {
            String appId = authorizationRequest.getRequestParameters().get(CDLConstants.AUTH_APP_ID);
            if (!StringUtils.isEmpty(appId)) {
                values.put(CDLConstants.AUTH_APP_ID, appId);
            }
        }

        String orgId = authorizationRequest.getRequestParameters().get(CDLConstants.ORG_ID);
        orgId = StringUtils.isNotBlank(orgId) ? orgId.trim() : null;
        String orgName = authorizationRequest.getRequestParameters().get(CDLConstants.ORG_NAME);
        orgName = StringUtils.isNotBlank(orgName) ? orgName.trim() : null;
        String externalSystemType = authorizationRequest.getRequestParameters().get(CDLConstants.EXTERNAL_SYSTEM_TYPE);
        externalSystemType = StringUtils.isNotBlank(externalSystemType) ? externalSystemType.trim() : null;

        if (orgId != null) {
            log.info(String.format("Got orgId = %s, orgName = %s, externalSystemType = %s", orgId, orgName,
                    externalSystemType));
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).");
        }

        try {
            byte[] bytes = digest.digest(values.toString().getBytes("UTF-8"));
            return String.format("%032x", new BigInteger(1, bytes));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding not available.  Fatal (should be in the JDK).");
        }
    }
}

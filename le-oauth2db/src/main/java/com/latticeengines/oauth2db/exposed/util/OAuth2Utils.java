package com.latticeengines.oauth2db.exposed.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.DefaultOAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.token.DefaultAccessTokenRequest;
import org.springframework.security.oauth2.client.token.grant.password.ResourceOwnerPasswordResourceDetails;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;

public final class OAuth2Utils {

    protected OAuth2Utils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(OAuth2Utils.class);

    @Deprecated
    @SuppressWarnings("unchecked")
    public static String extractHeaderToken(HttpServletRequest request) {
        Enumeration<String> headers = request.getHeaders("Authorization");
        while (headers.hasMoreElements()) { // typically there is only one (most
            // servers enforce that)
            String value = headers.nextElement();
            if ((value.toLowerCase().startsWith(OAuth2AccessToken.BEARER_TYPE.toLowerCase()))) {
                String authHeaderValue = value.substring(OAuth2AccessToken.BEARER_TYPE.length()).trim();
                int commaIndex = authHeaderValue.indexOf(',');
                if (commaIndex > 0) {
                    authHeaderValue = authHeaderValue.substring(0, commaIndex);
                }
                return authHeaderValue;
            }
        }

        return null;
    }

    public static String extractHeaderToken(String bearerToken) {
        if ((bearerToken.toLowerCase().startsWith(OAuth2AccessToken.BEARER_TYPE.toLowerCase()))) {
            String authHeaderValue = bearerToken.substring(OAuth2AccessToken.BEARER_TYPE.length()).trim();
            int commaIndex = authHeaderValue.indexOf(',');
            if (commaIndex > 0) {
                authHeaderValue = authHeaderValue.substring(0, commaIndex);
            }
            return authHeaderValue;
        }
        return null;
    }

    public static String extractTokenKey(String value) {
        if (value == null) {
            return null;
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).");
        }

        try {
            byte[] bytes = digest.digest(value.getBytes("UTF-8"));
            return String.format("%032x", new BigInteger(1, bytes));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding not available.  Fatal (should be in the JDK).");
        }
    }

    public static String getTenantName(String bearerToken, OAuthUserEntityMgr oAuthUserEntityMgr) {
        try {
            String token = OAuth2Utils.extractHeaderToken(bearerToken);
            if (token == null) {
                throw new LedpException(LedpCode.LEDP_23001);
            }
            String tokenId = OAuth2Utils.extractTokenKey(token);
            if (tokenId == null) {
                throw new LedpException(LedpCode.LEDP_23002);
            }
            String tenantName = oAuthUserEntityMgr.findTenantNameByAccessToken(tokenId);
            if (tenantName == null) {
                throw new LedpException(LedpCode.LEDP_23004);
            }
            return tenantName;
        } catch (Exception ex) {
            if (ex instanceof LedpException) {
                throw ex;
            }
            throw new LedpException(LedpCode.LEDP_23003, ex);
        }
    }

    @Deprecated
    public static String getTenantName(HttpServletRequest entity, OAuthUserEntityMgr oAuthUserEntityMgr) {
        try {
            String token = OAuth2Utils.extractHeaderToken(entity);
            if (token == null) {
                throw new LedpException(LedpCode.LEDP_23001);
            }
            String tokenId = OAuth2Utils.extractTokenKey(token);
            if (tokenId == null) {
                throw new LedpException(LedpCode.LEDP_23002);
            }
            String tenantName = oAuthUserEntityMgr.findTenantNameByAccessToken(tokenId);
            if (tenantName == null) {
                throw new LedpException(LedpCode.LEDP_23004);
            }
            return tenantName;
        } catch (Exception ex) {
            if (ex instanceof LedpException) {
                throw ex;
            }
            throw new LedpException(LedpCode.LEDP_23003, ex);
        }
    }

    public static Map<String, String> getAppId(String bearerToken, OAuthUserEntityMgr oAuthUserEntityMgr) {
        try {
            String token = OAuth2Utils.extractHeaderToken(bearerToken);
            if (token == null) {
                throw new LedpException(LedpCode.LEDP_23001);
            }
            Map<String, String> res = new HashMap<>();

            String appId = oAuthUserEntityMgr.findAppIdByAccessToken(token);
            if (StringUtils.isNotBlank(appId)) {
                res.put(CDLConstants.AUTH_APP_ID, appId);
            }
            return res;
        } catch (Exception ex) {
            log.error("Unable to find app_id");
            throw new LedpException(LedpCode.LEDP_23006, ex);
        }
    }

    public static Map<String, String> getOrgInfo(String bearerToken, OAuthUserEntityMgr oAuthUserEntityMgr) {
        String token = OAuth2Utils.extractHeaderToken(bearerToken);
        try {
            if (StringUtils.isBlank(token)) {
                throw new LedpException(LedpCode.LEDP_23001);
            }
            return oAuthUserEntityMgr.findOrgInfoByAccessToken(token);
        } catch (Exception ex) {
            log.error("Unable to find org info");
            throw new LedpException(LedpCode.LEDP_23007, ex);
        }
    }

    @Deprecated
    public static CustomerSpace getCustomerSpace(HttpServletRequest request, OAuthUserEntityMgr oAuthUserEntityMgr) {
        return CustomerSpace.parse(getTenantName(request, oAuthUserEntityMgr));
    }

    public static OAuth2RestTemplate getOauthTemplate(String authHostPort, String username, String password,
            String clientId) {
        return getOauthTemplate(authHostPort, username, password, clientId, null);
    }

    public static OAuth2RestTemplate getOauthTemplate(String authHostPort, String username, String password,
            String clientId, String appId) {
        return getOauthTemplate(authHostPort, username, password, clientId, appId, null, null);
    }

    public static OAuth2RestTemplate getOauthTemplate(String authHostPort, String username, String password,
            String clientId, String appId, String orgId, String externalSystemType) {
        ResourceOwnerPasswordResourceDetails resource = new ResourceOwnerPasswordResourceDetails();
        resource.setUsername(username);
        resource.setPassword(password);
        resource.setClientId(clientId);

        resource.setGrantType("password");
        authHostPort = authHostPort.endsWith("/") ? authHostPort.substring(0, authHostPort.length() - 1) : authHostPort;
        resource.setAccessTokenUri(authHostPort + "/oauth/token");

        DefaultAccessTokenRequest accessTokenRequest = new DefaultAccessTokenRequest();

        if (StringUtils.isNotBlank(appId)) {
            Map<String, List<String>> headers = new HashMap<>();
            List<String> appList = new ArrayList<>();
            appList.add(appId);
            headers.put(CDLConstants.AUTH_APP_ID, appList);
            accessTokenRequest.setHeaders(headers);
            accessTokenRequest.add(CDLConstants.AUTH_APP_ID, appId);
        }

        if (StringUtils.isNotBlank(orgId) && StringUtils.isNotBlank(externalSystemType)) {
            accessTokenRequest.add(CDLConstants.ORG_ID, orgId);
            accessTokenRequest.add(CDLConstants.EXTERNAL_SYSTEM_TYPE, externalSystemType);
        }
        OAuth2ClientContext context = new DefaultOAuth2ClientContext(accessTokenRequest);
        OAuth2RestTemplate newRestTemplate = new OAuth2RestTemplate(resource, context);
        newRestTemplate.setRequestFactory(HttpClientUtils.getSslBlindRequestFactory());
        return newRestTemplate;
    }

    public static OAuth2AccessToken getAccessToken(OAuth2RestTemplate oAuth2RestTemplate) {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        if (securityContext.getAuthentication() instanceof AnonymousAuthenticationToken) {
            synchronized (OAuth2Utils.class) {
                try {
                    SecurityContextHolder.clearContext();
                    return getAccessTokenInternal(oAuth2RestTemplate);
                } finally {
                    SecurityContextHolder.setContext(securityContext);
                }
            }
        } else {
            return getAccessTokenInternal(oAuth2RestTemplate);
        }
    }

    private static OAuth2AccessToken getAccessTokenInternal(OAuth2RestTemplate oAuth2RestTemplate) {
        OAuth2AccessToken token;
        SSLUtils.turnOffSSLNameVerification();
        try {
            token = oAuth2RestTemplate.getAccessToken();
        } finally {
            SSLUtils.turnOnSSLNameVerification();
        }
        return token;
    }

    public static String generatePassword() {
        RandomValueStringGenerator generator = new RandomValueStringGenerator(12);
        return generator.generate();
    }
}

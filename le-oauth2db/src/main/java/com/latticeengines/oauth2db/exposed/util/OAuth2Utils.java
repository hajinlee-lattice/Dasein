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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.oauth2.client.DefaultOAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.token.DefaultAccessTokenRequest;
import org.springframework.security.oauth2.client.token.grant.password.ResourceOwnerPasswordResourceDetails;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;

public class OAuth2Utils {

    private static final Log log = LogFactory.getLog(OAuth2Utils.class);
    private static final String APP_ID = "app_id";

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

    public static String getTenantName(HttpServletRequest request, OAuthUserEntityMgr oAuthUserEntityMgr) {
        try {
            String token = OAuth2Utils.extractHeaderToken(request);
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
            log.error("Can not get tenant!", ex);
            throw new LedpException(LedpCode.LEDP_23003, ex);
        }
    }

    public static CustomerSpace getCustomerSpace(HttpServletRequest request, OAuthUserEntityMgr oAuthUserEntityMgr) {
        return CustomerSpace.parse(getTenantName(request, oAuthUserEntityMgr));
    }

    public static OAuth2RestTemplate getOauthTemplate(String authHostPort, String username, String password,
            String clientId) {
        return getOauthTemplate(authHostPort, username, password, clientId, null);
    }

    public static OAuth2RestTemplate getOauthTemplate(String authHostPort, String username, String password,
            String clientId, String appId) {
        ResourceOwnerPasswordResourceDetails resource = new ResourceOwnerPasswordResourceDetails();
        resource.setUsername(username);
        resource.setPassword(password);
        resource.setClientId(clientId);

        resource.setGrantType("password");
        resource.setAccessTokenUri(authHostPort + "/oauth/token");

        DefaultAccessTokenRequest accessTokenRequest = new DefaultAccessTokenRequest();

        if (!StringUtils.isEmpty(appId)) {
            Map<String, List<String>> headers = new HashMap<>();
            List<String> appList = new ArrayList<>();
            appList.add(appId);
            headers.put(APP_ID, appList);
            accessTokenRequest.setHeaders(headers);
        }
        OAuth2ClientContext context = new DefaultOAuth2ClientContext(accessTokenRequest);
        OAuth2RestTemplate newRestTemplate = new OAuth2RestTemplate(resource, context);
        return newRestTemplate;
    }

    public static String generatePassword() {
        RandomValueStringGenerator generator = new RandomValueStringGenerator(12);
        return generator.generate();
    }

}

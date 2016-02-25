package com.latticeengines.oauth2db.exposed.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.oauth2.common.OAuth2AccessToken;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;

public class OAuth2Utils {

    private static final Log log = LogFactory.getLog(OAuth2Utils.class);

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

}

package com.latticeengines.security.exposed.jwt.handler.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;
import com.latticeengines.security.exposed.jwt.JwtManager;
import com.latticeengines.security.exposed.jwt.handler.JwtHandler;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;

@Component("zendeskJwtHandler")
public class ZendeskJwtHandler implements JwtHandler {

    @Value("${security.zendesk.jwt.redirecturl}")
    private String BASE_SSO_URL;
    @Value("${security.zendesk.jwt.secretkey}")
    private String SHARED_SECRET;

    public static final String HANDLER_NAME = "ZENDESK";
    public static final String ZENDESK_URL_QUERY_JWT_TOKEN_KEY = "jwt";
    public static final String ZENDESK_URL_QUERY_RETURN_TO_KEY = "return_to";

    public static final String ZENDESK_JWT_EMAIL_KEY = "email";
    public static final String ZENDESK_JWT_USER_NAME_KEY = "name";
    public static final String ZENDESK_JWT_ORGANIZATION = "organization";

    public static final String ZENDESK_JWT_REPLY_FUNCTION_REDIRECT = "redirect";

    @Override
    public String getName() {
        return HANDLER_NAME;
    }

    @PostConstruct
    public void register() {
        JwtManager.registerTokenHandler(HANDLER_NAME, this);
    }

    @Override
    public  JwtReplyParameters getJwtTokenWithFunction(GlobalAuthUser user, JwtRequestParameters reqParameters)
            throws LedpException {
        Map<String, String> parameters = reqParameters.getRequestParameters();
        String jwtString = getJwtToken(user, reqParameters);
        String return_to = parameters.get(ZENDESK_URL_QUERY_RETURN_TO_KEY);
        String redirectUrl = BASE_SSO_URL + "?" + ZENDESK_URL_QUERY_JWT_TOKEN_KEY + "=" + jwtString;

        if (return_to != null) {
            try {
                redirectUrl += "&" + ZENDESK_URL_QUERY_RETURN_TO_KEY + "=" + encode(return_to);
            } catch (UnsupportedEncodingException e) {
                throw new LedpException(LedpCode.LEDP_19008, e);
            }
        }
        JwtReplyParameters reply = new JwtReplyParameters();
        reply.setType("redirect");
        reply.setUrl(redirectUrl);
        return reply;
    }

    @Override
    public String getJwtToken(GlobalAuthUser user, JwtRequestParameters parameters) throws LedpException {
        String[] step1 = user.getEmail().split("@");
        String[] step2 = step1[1].split("\\.");
        String org = "";
        for (int i = 0; i < step2.length - 1; i++) {
            org += step2[i];
        }
        JWTClaimsSet jwtClaims = new JWTClaimsSet.Builder()
                .claim(ZENDESK_JWT_USER_NAME_KEY, user.getFirstName() + " " + user.getLastName())
                .claim(ZENDESK_JWT_EMAIL_KEY, user.getEmail()).claim(ZENDESK_JWT_ORGANIZATION, org)
                .jwtID(generateRandomString()).issueTime(new Date()).build();
        String jwtString;
        try {
            JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.HS256).contentType("text/plain").build();
            JWSObject jwsObject = new JWSObject(header, new Payload(jwtClaims.toJSONObject()));
            JWSSigner signer = new MACSigner(SHARED_SECRET.getBytes());
            jwsObject.sign(signer);
            jwtString = jwsObject.serialize();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19006, e);
        }
        return jwtString;
    }

    private String generateRandomString() {
        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
        return randomUUIDString;
    }

    private long getUnixTimestamp() {
        long unixTimestamp = Instant.now().getEpochSecond();
        return unixTimestamp;
    }

    private String encode(String url) throws UnsupportedEncodingException {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new LedpException(LedpCode.LEDP_19006, e);
        }
    }

    public void validateJwtParameters(GlobalAuthUser user, JwtRequestParameters reqParameters) throws LedpException {
        Map<String, String> parameters = reqParameters.getRequestParameters();
        if (StringUtils.isBlank(user.getFirstName()) && StringUtils.isBlank(user.getLastName())) {
            throw new LedpException(LedpCode.LEDP_19009);
        }
        if (StringUtils.isBlank(user.getEmail())) {
            throw new LedpException(LedpCode.LEDP_19010, new String[] { ZENDESK_JWT_EMAIL_KEY });
        }
        if (parameters.containsKey(ZENDESK_URL_QUERY_RETURN_TO_KEY)) {
            if (StringUtils.isBlank(parameters.get(ZENDESK_URL_QUERY_RETURN_TO_KEY))) {
                throw new LedpException(LedpCode.LEDP_19011);
            }
        } else {
            throw new LedpException(LedpCode.LEDP_19011);
        }
    }
}

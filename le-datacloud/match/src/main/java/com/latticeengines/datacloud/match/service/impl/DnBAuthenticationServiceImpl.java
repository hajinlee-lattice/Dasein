package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import com.latticeengines.common.exposed.util.RestTemplateUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;

@Component
public class DnBAuthenticationServiceImpl implements DnBAuthenticationService {
    private static final Log log = LogFactory.getLog(DnBAuthenticationServiceImpl.class);

    @Value("${datacloud.dnb.transactionalmatch.api.key}")
    private String realtimeKey;

    @Value("${datacloud.dnb.bulkmatch.api.key}")
    private String bulkKey;

    @Value("${datacloud.dnb.password.encrypted}")
    private String passwd;

    @Value("${datacloud.dnb.user.header}")
    private String userHeader;

    @Value("${datacloud.dnb.password.header}")
    private String passwdHeader;

    @Value("${datacloud.dnb.authority.url}")
    private String authorityUrl;

    @Value("${datacloud.dnb.application.id.header}")
    private String applicationIdHeader;

    @Value("${datacloud.dnb.application.id}")
    private String applicationId;

    @Value("${datacloud.dnb.token.cache.expiration.time}")
    private int tokenCacheExpirationTime;

    private RestTemplate restTemplate = RestTemplateUtils.newSSLBlindRestTemplate();
    private LoadingCache<DnBKeyType, String> tokenCache;

    @PostConstruct
    public void initialize() throws Exception {
        tokenCache = //
                CacheBuilder.newBuilder()//
                        .expireAfterWrite(tokenCacheExpirationTime, TimeUnit.MINUTES)//
                        .build(new CacheLoader<DnBKeyType, String>() {
                            public String load(DnBKeyType type) throws Exception {
                                return requestTokenFromDnB(type);
                            }
                        });
    }

    protected HttpEntity<String> authorityRequestEntity(String apiKey, String passwd) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(userHeader, apiKey);
        headers.add(passwdHeader, passwd);
        return new HttpEntity<>("", headers);
    }

    private String obtainAuthorizationReponseBody(DnBKeyType type) throws IOException {
        String apikey;
        if (type == DnBKeyType.REALTIME) {
            apikey = realtimeKey;
        } else {
            apikey = bulkKey;
        }
        ResponseEntity<String> res = restTemplate.postForEntity(authorityUrl, authorityRequestEntity(apikey, passwd),
                String.class);
        return res.getBody().toString();
    }

    private String retrieveTokenFromResponseBody(String body) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(body);
        String token = (String) ((JSONObject) obj.get("AuthenticationDetail")).get("Token");
        if (token == null) {
            throw new RuntimeException(String.format("Failed to parse token from response %s!", body));
        }
        return token;
    }

    @Override
    public String requestToken(DnBKeyType type) {
        try {
            return tokenCache.get(type);
        } catch (ExecutionException e) {
            log.error(e);
            throw new RuntimeException("Failed to get the token from tokenCache!");
        }
    }

    @Override
    public String refreshAndGetToken(DnBKeyType type) {
        tokenCache.refresh(type);
        log.info("DnB token is expired, retry it.");
        return requestToken(type);
    }

    private String requestTokenFromDnB(DnBKeyType type) {

        String token = "";
        try {
            String response = obtainAuthorizationReponseBody(type);
            token = retrieveTokenFromResponseBody(response);
        } catch (ParseException | IOException e) {
            log.error(e);
        }

        return token;
    }
}

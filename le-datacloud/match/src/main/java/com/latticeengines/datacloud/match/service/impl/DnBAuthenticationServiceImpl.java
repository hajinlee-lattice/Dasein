package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.RestApiClient;

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

    @Value("${datacloud.dnb.token.cache.expiration.duration.minute}")
    private int tokenCacheExpirationTime;

    @Value("${datacloud.dnb.authentication.token.jsonpath}")
    private String tokenJsonPath;

    private LoadingCache<DnBKeyType, String> tokenCache;

    private RestApiClient dnbClient = new RestApiClient();

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
        switch (type) {
        case REALTIME:
            return dnbClient.post(authorityRequestEntity(realtimeKey, passwd), authorityUrl);
        case BATCH:
            return dnbClient.post(authorityRequestEntity(bulkKey, passwd), authorityUrl);
        default:
            throw new UnsupportedOperationException(
                    String.format("DnBKeyType %s is not supported in DnBAuthenticationService.", type.name()));
        }
    }

    private String retrieveTokenFromResponseBody(String body) throws ParseException {
        String token = JsonPath.parse(body).read(tokenJsonPath);
        if (token == null) {
            throw new RuntimeException(String.format("Failed to parse token from response %s!", body));
        }
        return token;
    }

    @Override
    public String requestToken(DnBKeyType type) {
        String token;
        try {
            token = tokenCache.get(type);
        } catch (ExecutionException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_25027);
        }
        if (StringUtils.isEmpty(token)) {
            throw new LedpException(LedpCode.LEDP_25027);
        }
        return token;
    }

    @Override
    public void refreshToken(DnBKeyType type) {
        tokenCache.refresh(type);
        log.info("DnB token is expired, retry it.");
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

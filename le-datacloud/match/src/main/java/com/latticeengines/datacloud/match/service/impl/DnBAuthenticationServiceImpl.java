package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component("dnbAuthenticationService")
public class DnBAuthenticationServiceImpl implements DnBAuthenticationService {
    private static final Logger log = LoggerFactory.getLogger(DnBAuthenticationServiceImpl.class);

    @Value("${datacloud.dnb.realtime.api.key}")
    private String realtimeKey;

    @Value("${datacloud.dnb.bulk.api.key}")
    private String bulkKey;

    @Value("${datacloud.dnb.realtime.password.encrypted}")
    private String realtimePwd;

    @Value("${datacloud.dnb.bulk.password.encrypted}")
    private String bulkPwd;

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

    @Autowired
    private ApplicationContext applicationContext;

    private RestApiClient dnbClient;

    @PostConstruct
    public void initialize() throws Exception {
        dnbClient = RestApiClient.newExternalClient(applicationContext);
        tokenCache = //
                CacheBuilder.newBuilder()//
                        .expireAfterWrite(tokenCacheExpirationTime, TimeUnit.MINUTES)//
                        .build(new CacheLoader<DnBKeyType, String>() {
                            @Override
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
            return dnbClient.post(authorityRequestEntity(realtimeKey, realtimePwd), authorityUrl);
        case BATCH:
            return dnbClient.post(authorityRequestEntity(bulkKey, bulkPwd), authorityUrl);
        default:
            throw new UnsupportedOperationException(
                    String.format("DnBKeyType %s is not supported in DnBAuthenticationService.", type.name()));
        }
    }

    private String retrieveTokenFromResponseBody(String body) {
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
            log.error(e.getMessage(), e);
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
        log.info("DnB token is expired, refresh it.");
    }

    private String requestTokenFromDnB(DnBKeyType type) {
        String token = "";
        try {
            String response = obtainAuthorizationReponseBody(type);
            token = retrieveTokenFromResponseBody(response);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        log.info("Refreshed DnB token to be {}", token);

        return token;
    }
}

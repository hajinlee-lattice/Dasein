package com.latticeengines.security.service.impl;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.auth.IDaaSExternalSession;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.util.LoginUtils;

@Service("iDaasService")
public class IDaaSServiceImpl implements IDaaSService {

    private static final Logger log = LoggerFactory.getLogger(IDaaSServiceImpl.class);

    private static final String DCP_PRODUCT = "Data Cloud Portal";
    private static final String DCP_ROLE = "DATA_CLOUD_PORTAL_ACCESS";

    private RestTemplate restTemplate = HttpClientUtils.newJsonRestTemplate();

    @Inject
    private SessionService sessionService;

    @Inject
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Inject
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Inject
    private TenantService tenantService;

    @Value("${security.idaas.api.url}")
    private String apiUrl;

    @Value("${security.idaas.client.id}")
    private String clientId;

    @Value("${security.idaas.client.secret.encrypted}")
    private String clientSecret;

    @Value("${security.idaas.enabled}")
    private boolean enabled;

    private volatile boolean initialized = false;

    @Override
    public LoginDocument login(Credentials credentials) {
        initialize();
        LoginDocument doc = new LoginDocument();
        doc.setErrors(Collections.singletonList("Failed to authenticate the user."));
        boolean authenticated = authenticate(credentials);
        if (authenticated) {
            IDaaSUser iDaaSUser = getIDaaSUser(credentials.getUsername());
            boolean hasAccess = hasAccessToApp(iDaaSUser);
            if (hasAccess) {
                String email = iDaaSUser.getEmailAddress();
                if (StringUtils.isBlank(email)) {
                    email = credentials.getUsername();
                }
                IDaaSExternalSession externalSession = new IDaaSExternalSession();
                externalSession.setIssuer("IDaaS");
                Ticket ticket = null;
                try {
                    ticket = sessionService.authenticateSamlUser(email, externalSession);
                } catch (Exception e) {
                    log.warn("Failed to generate ticket for external session.", e);
                }
                doc = LoginUtils.generateLoginDoc(ticket, gaUserEntityMgr, gaTicketEntityMgr, tenantService);
                doc.setFirstName(iDaaSUser.getFirstName());
                doc.setLastName(iDaaSUser.getLastName());
                doc.getResult().setMustChangePassword(false);
                doc.getResult().setPasswordLastModified(System.currentTimeMillis());
            } else {
                doc.setErrors(Collections.singletonList("The user does not have access to this application."));
            }
        }
        return doc;
    }

    private boolean authenticate(Credentials credentials) {
        if (enabled) {
            IDaaSCredentials iDaaSCreds = new IDaaSCredentials();
            iDaaSCreds.setUserName(credentials.getUsername());
            iDaaSCreds.setPassword(credentials.getPassword());
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
            try {
                return retryTemplate.execute(ctx -> {
                    try (PerformanceTimer timer = new PerformanceTimer("Authenticate user against IDaaS.")) {
                        ResponseEntity<JsonNode> response = restTemplate.postForEntity(authenticateUri(), iDaaSCreds, JsonNode.class);
                        if (HttpStatus.OK.equals(response.getStatusCode())) {
                            return true;
                        } else {
                            log.warn("Cannot authenticate user {} against IDaaS: {}", credentials.getUsername(), response.getBody());
                            throw new RuntimeException(JsonUtils.serialize(response.getBody()));
                        }
                    } catch (HttpClientErrorException.Unauthorized e) {
                        log.warn("Failed to authenticate user {}: {}", credentials.getUsername(), e.getMessage());
                        return false;
                    }
                });
            } catch (Exception e) {
                log.warn("Cannot authenticate user {} against IDaaS", credentials.getUsername(), e);
                return false;
            }
        } else {
            String email = credentials.getUsername();
            if (StringUtils.isNotBlank(email) && (email.toLowerCase().endsWith("dnb.com")
                    || email.equalsIgnoreCase("dcp_dev@lattice-engines.com"))) {
                log.info("Blindly authenticate IDaaS user {}, as IDaaS is disabled.", credentials.getUsername());
                return true;
            } else {
                log.info("Blindly reject IDaaS user {}, as IDaaS is disabled.", credentials.getUsername());
                return false;
            }
        }
    }

    @Override
    public IDaaSUser getIDaaSUser(String email) {
        if (enabled) {
            IDaaSUser user = null;
            try {
                RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
                user = retryTemplate.execute(ctx -> {
                    try (PerformanceTimer timer = new PerformanceTimer("Check user detail in IDaaS.")) {
                        ResponseEntity<IDaaSUser> response = restTemplate.getForEntity(userUri(email), IDaaSUser.class);
                        return response.getBody();
                    } catch (HttpClientErrorException.Unauthorized e) {
                        log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                        return null;
                    }
                });
            } catch (Exception e) {
                log.warn("Failed to check user detail in IDaaS for {}", email, e);
            }
            return user;
        } else {
            IDaaSUser user = new IDaaSUser();
            user.setEmailAddress(email);
            user.setUserName(email);
            user.setApplications(Collections.singletonList(DCP_PRODUCT));
            user.setRoles(Collections.singletonList(DCP_ROLE));
            return user;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IDaaSUser updateIDaaSUser(IDaaSUser user) {
        initialize();
        String email = user.getEmailAddress();
        IDaaSUser returnedUser = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
            returnedUser = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("update user in IDaaS.")) {
                    HttpEntity entity = new HttpEntity(user);
                    ResponseEntity<IDaaSUser> responseEntity = restTemplate.exchange(userUri(email),
                            HttpMethod.PUT, entity, IDaaSUser.class);
                    return responseEntity.getBody();
                } catch (HttpClientErrorException.Unauthorized e) {
                    log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to update user detail in IDaaS for {}", email, e);
        }
        return returnedUser;
    }

    @Override
    public IDaaSUser createIDaaSUser(IDaaSUser user) {
        initialize();
        String email = user.getEmailAddress();
        IDaaSUser returnedUser = null;
        try {
            RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
            returnedUser = retryTemplate.execute(ctx -> {
                try (PerformanceTimer timer = new PerformanceTimer("create user in IDaaS.")) {
                    ResponseEntity<IDaaSUser> responseEntity = restTemplate.postForEntity(createUserUri(),
                            user, IDaaSUser.class);
                    return responseEntity.getBody();
                } catch (HttpClientErrorException.Unauthorized e) {
                    log.warn("Failed to authenticate user {}: {}", email, e.getMessage());
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Failed to create user detail in IDaaS for {}", email, e);
        }
        return returnedUser;
    }

    private boolean hasAccessToApp(IDaaSUser user) {
        return user.getApplications().contains(DCP_PRODUCT) || user.getRoles().contains(DCP_ROLE);
    }

    private void refreshOAuthTokens() {
        Map<String, String> payload = ImmutableMap.of("grant_type", "client_credentials");
        RestTemplate restTemplate = HttpClientUtils.newJsonRestTemplate();
        String headerValue = String.format("client_id:%s,client_secret:%s", clientId, clientSecret);
        ClientHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(headerValue);
        restTemplate.setInterceptors(Collections.singletonList(interceptor));
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(3);
        JsonNode jsonNode = retryTemplate.execute(ctx -> {
            try(PerformanceTimer timer = new PerformanceTimer("Get OAuth2 token from IDaaS.")) {
                ResponseEntity<JsonNode> response = restTemplate.postForEntity(oauthTokenUri(), payload, JsonNode.class);
                JsonNode body = response.getBody();
                if (body != null) {
                    return body;
                } else {
                    throw new RuntimeException("Get empty response from oauth request.");
                }
            }
        });
        String accessToken = jsonNode.get("access_token").asText();
        String refreshToken = jsonNode.get("refresh_token").asText();
        log.info("IDaaS OAuth AssessToken={}, RefreshToken={}", accessToken, refreshToken);

        setOauthToken(accessToken);
    }

    private void setOauthToken(String accessToken) {
        String headerValue = "Bearer " + accessToken;
        ClientHttpRequestInterceptor authHeader = new AuthorizationHeaderHttpRequestInterceptor(headerValue);
        List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    private void initialize() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
                    scheduler.setPoolSize(1);
                    scheduler.setThreadNamePrefix("idaas-oauth-token");
                    scheduler.initialize();
                    scheduler.scheduleWithFixedDelay(this::refreshOAuthTokens, TimeUnit.DAYS.toMillis(1));
                    initialized = true;
                }
            }
        }
    }

    private URI oauthTokenUri() {
        return URI.create(apiUrl + "/oauth2/v3/token");
    }

    private URI authenticateUri() {
        return URI.create(apiUrl + "/user/v2/authenticate");
    }

    private URI userUri(String email) {
        return URI.create(apiUrl + "/user/" + email);
    }

    private URI createUserUri() {
        return URI.create(apiUrl + "/user");
    }

}

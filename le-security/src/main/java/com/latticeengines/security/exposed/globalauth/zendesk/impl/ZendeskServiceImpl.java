package com.latticeengines.security.exposed.globalauth.zendesk.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryException;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.zendesk.ZendeskUser;
import com.latticeengines.security.exposed.globalauth.zendesk.ZendeskService;

@Service("zendeskService")
public class ZendeskServiceImpl implements ZendeskService {

    private static final Set<Class<? extends Throwable>> RETRY_EXCEPTIONS = ImmutableSet.of(
            AsyncRequestTimeoutException.class,
            ResourceAccessException.class,
            RestClientException.class,
            java.io.IOException.class
    );

    private static final String BASE_PATH = "api/v2";
    private static final String USER_ENDPOINT_PREFIX = "/users";

    private static final class Endpoint {
        static final String CREATE_OR_UPDATE_USER = "/create_or_update.json";
        static final String ID_FORMAT = "/%d.json";
        static final String SET_PASSWORD_FORMAT = "/%d/password.json";
        static final String SEARCH = "/search.json";
    }

    @Value("${security.zendesk.url}")
    private String baseUrl;
    @Value("${security.zendesk.email}")
    private String email;
    @Value("${security.zendesk.encryptedApiToken}")
    private String apiToken;
    @Value("${proxy.retry.initialwaitmsec:500}")
    private long initialWaitMsec;
    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;
    @Value("${proxy.retry.maxattempts:5}")
    private int maxAttempts;
    private RestTemplate template = HttpClientUtils.newJsonRestTemplate();
    private RetryTemplate retryTemplate;

    public ZendeskServiceImpl() {
    }

    public ZendeskServiceImpl(String baseUrl, String email, String apiToken) {
        this.baseUrl = baseUrl;
        this.email = email;
        this.apiToken = apiToken;

        setupAuthentication();
        setupRetryTemplate();
    }

    @Override
    public ZendeskUser createOrUpdateUser(ZendeskUser user) {
        checkUserIdOrEmail(user);
        String url = getUserUrl(Endpoint.CREATE_OR_UPDATE_USER);
        HttpEntity<UserWrapper> request = new HttpEntity<>(new UserWrapper(user));
        ResponseEntity<UserWrapper> response = makeApiCall(url, HttpMethod.POST, request, UserWrapper.class);
        checkResponseStatus(response, HttpStatus.OK, HttpStatus.CREATED);
        checkResponseBody(response.getBody());
        return response.getBody().getUser();
    }

    @Override
    public ZendeskUser updateUser(ZendeskUser user) {
        checkUserId(user);
        String url = getUserUrl(String.format(Endpoint.ID_FORMAT, user.getId()));
        HttpEntity<UserWrapper> request = new HttpEntity<>(new UserWrapper(user));
        ResponseEntity<UserWrapper> response = makeApiCall(url, HttpMethod.PUT, request, UserWrapper.class);
        checkResponseStatus(response, HttpStatus.OK);
        checkResponseBody(response.getBody());
        return response.getBody().getUser();
    }

    @Override
    public ZendeskUser findUserByEmail(String email) {
        Preconditions.checkNotNull(email);
        String url = getUserUrl(Endpoint.SEARCH);
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromHttpUrl(url)
                .queryParam("query", String.format("email:%s", email));
        ResponseEntity<UserListWrapper> response = makeApiCall(
                builder.toUriString(), HttpMethod.GET, null, UserListWrapper.class);
        checkResponseStatus(response, HttpStatus.OK);

        List<ZendeskUser> users = checkResponseBody(response.getBody());
        return users.size() > 0 ? users.get(0) : null;
    }

    @Override
    public void suspendUser(long userId) {
        setSuspensionStatus(userId, true);
    }

    @Override
    public void unsuspendUser(long userId) {
        setSuspensionStatus(userId, false);
    }

    @Override
    public void suspendUserByEmail(String email) {
        setSuspensionStatus(email, true);
    }

    @Override
    public void unsuspendUserByEmail(String email) {
        setSuspensionStatus(email, false);
    }

    @Override
    public void setUserPassword(long userId, String password) {
        String url = getUserUrl(String.format(Endpoint.SET_PASSWORD_FORMAT, userId));
        HttpEntity<?> request = new HttpEntity<>(Collections.singletonMap("password", password));
        ResponseEntity<Void> response = makeApiCall(url, HttpMethod.POST, request, Void.class);
        checkResponseStatus(response, HttpStatus.OK);
    }

    private <T> ResponseEntity<T> makeApiCall(String url, HttpMethod method, HttpEntity<?> request, Class<T> clz) {
        try {
            return retryApiCall(url, method, request, clz);
        } catch (HttpClientErrorException e) {
            switch (e.getStatusCode()) {
                case TOO_MANY_REQUESTS:
                    // rate limit exceeded
                    throw new LedpException(LedpCode.LEDP_00005, e);
                case NOT_FOUND:
                    throw new LedpException(LedpCode.LEDP_19003, e);
                case BAD_REQUEST:
                    throw new LedpException(LedpCode.LEDP_25037, e);
                default:
                    throw new LedpException(LedpCode.LEDP_00007, e);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00007, e);
        }
    }

    private <T> ResponseEntity<T> retryApiCall(String url, HttpMethod method, HttpEntity<?> request, Class<T> clz) {
        try {
            return retryTemplate.execute((context) -> template.exchange(url, method, request, clz));
        } catch (RetryException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpClientErrorException) {
                throw ((HttpClientErrorException) cause);
            } else {
                throw new LedpException(LedpCode.LEDP_00007, e);
            }
        }
    }

    private RetryTemplate getRetryTemplate() {
        // cannot use proxy due to circular dependency
        RetryTemplate template = new RetryTemplate();
        Map<Class<? extends Throwable>, Boolean> retryExceptionMap = RETRY_EXCEPTIONS
                .stream().collect(Collectors.toMap(e -> e, e -> true));
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, retryExceptionMap, true);
        template.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        template.setBackOffPolicy(backOffPolicy);
        template.setThrowLastExceptionOnExhausted(true);
        template.registerListener(new ZendeskRetryListener());
        return template;
    }

    private void setSuspensionStatus(long userId, boolean suspended) {
        ZendeskUser request = new ZendeskUser();
        request.setId(userId);
        request.setSuspended(suspended);
        updateUser(request);
    }

    /**
     * Find Zendesk user by email and update with the input suspension status
     *
     * @param email     target user email
     * @param suspended input suspension status
     */
    private void setSuspensionStatus(String email, boolean suspended) {
        ZendeskUser user = findUserByEmail(email);
        if (user == null) {
            // no such user
            return;
        }
        setSuspensionStatus(user.getId(), suspended);
    }

    private String getUserUrl(String path) {
        return String.format("%s%s%s", baseUrl,
                (baseUrl.endsWith("/") ? "" : "/") + BASE_PATH + USER_ENDPOINT_PREFIX, path);
    }

    private void checkUserId(ZendeskUser user) {
        Preconditions.checkNotNull(user);
        Preconditions.checkNotNull(user.getId());
    }

    /**
     * user either contains ID or email field
     * user contains name field
     */
    private void checkUserIdOrEmail(ZendeskUser user) {
        Preconditions.checkNotNull(user);
        Preconditions.checkNotNull(user.getName());
        Preconditions.checkArgument(user.getId() != null || user.getEmail() != null);
    }

    private void checkResponseBody(UserWrapper wrapper) {
        Preconditions.checkNotNull(wrapper);
        Preconditions.checkNotNull(wrapper.getUser());
    }

    @NotNull
    private List<ZendeskUser> checkResponseBody(UserListWrapper wrapper) {
        Preconditions.checkNotNull(wrapper);
        Preconditions.checkNotNull(wrapper.getUsers());
        return wrapper.getUsers();
    }

    private void checkResponseStatus(ResponseEntity<?> response, HttpStatus... desireStatuses) {
        for (HttpStatus status : desireStatuses) {
            if (status == response.getStatusCode()) {
                return;
            }
        }
        throw new LedpException(LedpCode.LEDP_00007);
    }

    @PostConstruct
    private void setupAuthentication() {
        // basic auth with apiToken
        template.getInterceptors().add(new BasicAuthorizationInterceptor(email + "/token", apiToken));
    }

    @PostConstruct
    private void setupRetryTemplate() {
        this.retryTemplate = getRetryTemplate();
    }

    private static class ZendeskRetryListener extends RetryListenerSupport {
        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            if (throwable instanceof HttpClientErrorException) {
                HttpClientErrorException clientException = ((HttpClientErrorException) throwable);
                HttpStatus status = clientException.getStatusCode();
                if (status.is4xxClientError() && status != HttpStatus.TOO_MANY_REQUESTS) {
                    // only retry on rate limit error if it is a client error
                    context.setExhaustedOnly();
                }
            }
        }
    }

    /**
     * wrapper class (for HTTP request & response body)
     */
    private static class UserWrapper {
        private ZendeskUser user;

        @SuppressWarnings("unused")
        UserWrapper() {
        }

        UserWrapper(ZendeskUser user) {
            this.user = user;
        }

        public ZendeskUser getUser() {
            return user;
        }

        @SuppressWarnings("unused")
        public void setUser(ZendeskUser user) {
            this.user = user;
        }
    }

    /**
     * wrapper class (for HTTP request & response body)
     */
    private static class UserListWrapper {
        private List<ZendeskUser> users;

        @SuppressWarnings("unused")
        UserListWrapper() {
        }

        @SuppressWarnings("unused")
        UserListWrapper(List<ZendeskUser> users) {
            this.users = users;
        }

        public List<ZendeskUser> getUsers() {
            return users;
        }

        @SuppressWarnings("unused")
        public void setUsers(List<ZendeskUser> users) {
            this.users = users;
        }
    }
}

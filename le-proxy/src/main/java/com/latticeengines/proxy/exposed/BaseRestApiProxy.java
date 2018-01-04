package com.latticeengines.proxy.exposed;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public abstract class BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(BaseRestApiProxy.class);
    private RestTemplate restTemplate;
    private String hostport;
    private String rootpath;

    @Value("${proxy.retry.initialwaitmsec:1000}")
    private long initialWaitMsec;

    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;

    @Value("${proxy.retry.maxAttempts:10}")
    private int maxAttempts;

    // Used to call external API because there is no standardized error handler
    protected BaseRestApiProxy() {
        this.restTemplate = HttpClientUtils.newRestTemplate();
    }

    protected BaseRestApiProxy(String hostport) {
        this(hostport, null);
    }

    protected BaseRestApiProxy(String hostport, String rootpath, Object... urlVariables) {
        this.hostport = hostport;
        this.rootpath = StringUtils.isEmpty(rootpath) ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        this.restTemplate = HttpClientUtils.newRestTemplate();
        this.restTemplate.setErrorHandler(new GetResponseErrorHandler());
        setMagicAuthHeader();
    }

    void setMagicAuthHeader() {
        MagicAuthenticationHeaderHttpRequestInterceptor authHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof MagicAuthenticationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    void setAuthHeader(String authToken) {
        AuthorizationHeaderHttpRequestInterceptor authHeader = new AuthorizationHeaderHttpRequestInterceptor(authToken);
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    void setAuthInterceptor(AuthorizationHeaderHttpRequestInterceptor authHeader) {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    protected void cleanupAuthHeader() {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.removeIf(i -> i instanceof MagicAuthenticationHeaderHttpRequestInterceptor);
        restTemplate.setInterceptors(interceptors);
    }

    protected void setErrorHandler(ResponseErrorHandler handler) {
        restTemplate.setErrorHandler(handler);
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, true);
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz,
            final boolean logBody) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                String msg;
                if (logBody) {
                    msg = String.format("Invoking %s by posting to url %s with body %s.  (Attempt=%d)", method, url,
                            body, context.getRetryCount() + 1);
                } else {
                    msg = String.format("Invoking %s by posting to url %s.  (Attempt=%d)", method, url,
                            context.getRetryCount() + 1);
                }
                log.info(msg);
                return restTemplate.postForObject(url, body, returnValueClazz);
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected <T, B> T postWithRetries(final String method, final String url, final B body,
            final Class<T> returnValueClazz) {
        return postWithRetries(method, url, body, returnValueClazz, true);
    }

    protected <T, B> T postWithRetries(final String method, final String url, final B body,
            final Class<T> returnValueClazz, final boolean logBody) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                String msg;
                if (logBody) {
                    msg = String.format("Invoking %s by posting to url %s with body %s.  (Attempt=%d)", method, url,
                            body, context.getRetryCount() + 1);
                } else {
                    msg = String.format("Invoking %s by posting to url %s.  (Attempt=%d)", method, url,
                            context.getRetryCount() + 1);
                }
                log.info(msg);
                return restTemplate.postForObject(url, body, returnValueClazz);
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected <T> T postMultiPart(final String method, final String url, final MultiValueMap<String, Object> parts,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                String msg = String.format("Invoking %s by posting to url %s.  (Attempt=%d)", method, url,
                        context.getRetryCount() + 1);
                log.info(msg);
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts, headers);
                RestTemplate newRestTemplate = HttpClientUtils.newRestTemplate();
                List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
                for (ClientHttpRequestInterceptor interceptor : restTemplate.getInterceptors()) {
                    if (interceptor instanceof AuthorizationHeaderHttpRequestInterceptor
                            || interceptor instanceof MagicAuthenticationHeaderHttpRequestInterceptor) {
                        interceptors.add(interceptor);
                    }
                }
                newRestTemplate.setInterceptors(interceptors);
                ResponseEntity<T> response = newRestTemplate.exchange(url, HttpMethod.POST, requestEntity,
                        returnValueClazz);
                return response.getBody();
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected <T> T postForUrlEncoded(final String method, final String url, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                String msg = String.format("Invoking %s by posting to url %s.  (Attempt=%d)", method, url,
                        context.getRetryCount() + 1);
                log.info(msg);
                String baseUrl = url.substring(0, url.indexOf("?"));
                String params = url.substring(url.indexOf("?") + 1);
                RestTemplate newRestTemplate = HttpClientUtils.newFormURLEncodedRestTemplate();

                T response = newRestTemplate.postForObject(baseUrl, params, returnValueClazz);
                return response;
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected <T> T postForEntity(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                log.info(String.format("Invoking %s by posting from url %s with http headers.  (Attempt=%d)", method,
                        url, context.getRetryCount() + 1));
                return restTemplate.postForEntity(url, entity, returnValueClazz).getBody();
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    private void logError(Exception e, String method) {
        log.error(String.format("%s: Remote call failure", method), e);
    }

    protected <B> void put(final String method, final String url, final B body) {
        put(method, url, body, true);
    }

    protected <B> void put(final String method, final String url, final B body, boolean logBody) {
        RetryTemplate retry = getRetryTemplate();
        retry.execute((RetryCallback<Void, RuntimeException>) context -> {
            try {
                if (logBody) {
                    log.info(String.format("Invoking %s by putting to url %s with body %s.  (Attempt=%d)", method, url,
                            body, context.getRetryCount() + 1));
                } else {
                    log.info(String.format("Invoking %s by putting to url %s.  (Attempt=%d)", method, url,
                            context.getRetryCount() + 1));
                }
                restTemplate.put(url, body);
                return null;
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }

        });
    }

    protected <T> T get(final String method, final String url, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                log.info(String.format("Invoking %s by getting from url %s.  (Attempt=%d)", method, url,
                        context.getRetryCount() + 1));
                return restTemplate.getForObject(url, returnValueClazz);
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected <T> T get(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                log.info(String.format("Invoking %s by getting from url %s with http headers.  (Attempt=%d)", method,
                        url, context.getRetryCount() + 1));
                return restTemplate.exchange(url, HttpMethod.GET, entity, returnValueClazz).getBody();
            } catch (LedpException e) {
                context.setExhaustedOnly();
                logError(e, method);
                throw e;
            } catch (Exception e) {
                logError(e, method);
                throw e;
            }
        });
    }

    protected void delete(final String method, final String url) {
        RetryTemplate retry = getRetryTemplate();
        retry.execute(new RetryCallback<Void, RuntimeException>() {
            @Override
            public Void doWithRetry(RetryContext context) {
                try {
                    log.info(String.format("Invoking %s by deleting from url %s.  (Attempt=%d)", method, url,
                            context.getRetryCount() + 1));
                    restTemplate.delete(url);
                    return null;
                } catch (LedpException e) {
                    context.setExhaustedOnly();
                    logError(e, method);
                    throw e;
                } catch (Exception e) {
                    logError(e, method);
                    throw e;
                }
            }
        });
    }

    private RetryTemplate getRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxAttempts);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }

    protected String constructUrl() {
        return constructUrl(null);
    }

    protected String constructUrl(Object path, Object... variables) {
        if (hostport == null || hostport.equals("")) {
            throw new NullPointerException("hostport must be set");
        }

        String end = rootpath;
        if (path != null) {
            String expandedPath = new UriTemplate(path.toString()).expand(variables).toString();
            end = combine(rootpath, expandedPath);
        }
        return combine(hostport, end);
    }

    private String combine(Object... parts) {
        List<String> toCombine = new ArrayList<>();
        for (int i = 0; i < parts.length; ++i) {
            String part = parts[i].toString();
            if (i != 0) {
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
            }

            if (i != parts.length - 1) {
                if (part.endsWith("/")) {
                    part = part.substring(0, part.length() - 2);
                }
            }
            toCombine.add(part);
        }
        return StringUtils.join(toCombine, "/");
    }

    public String getRootpath() {
        return rootpath;
    }

    public String getHostport() {
        return hostport;
    }

    public void setHostport(String hostport) {
        this.hostport = hostport;
    }

    protected void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        log.info("set " + getClass().getSimpleName() + " maxattemps to " + maxAttempts);
    }

    void enforceSSLNameVerification() {
        restTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
        setMagicAuthHeader();
    }
}

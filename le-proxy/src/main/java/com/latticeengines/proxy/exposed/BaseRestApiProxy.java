package com.latticeengines.proxy.exposed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
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
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriTemplate;

import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(BaseRestApiProxy.class);
    private static final String MONO = "mono";
    private static final String FLUX = "flux";

    private RestTemplate restTemplate;
    private WebClient webClient;
    private String hostport;
    private String rootpath;

    private long initialWaitMsec = 1000;
    private double multiplier = 2;
    private int maxAttempts = 10;

    private ConcurrentMap<String, String> headers = new ConcurrentHashMap<>();

    // Used to call external API because there is no standardized error handler
    protected BaseRestApiProxy() {
        this.restTemplate = HttpClientUtils.newRestTemplate();
        this.webClient = HttpClientUtils.newWebClient();
        initialConfig();
    }

    protected BaseRestApiProxy(String hostport) {
        this(hostport, null);
    }

    protected BaseRestApiProxy(String hostport, String rootpath, Object... urlVariables) {
        this.hostport = hostport;
        this.rootpath = StringUtils.isEmpty(rootpath) ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        this.restTemplate = HttpClientUtils.newRestTemplate();
        this.restTemplate.setErrorHandler(new GetResponseErrorHandler());
        this.webClient = HttpClientUtils.newWebClient();
        setMagicAuthHeader();
        initialConfig();
    }

    private void initialConfig() {
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.maxAttempts"))) {
            this.maxAttempts = Integer.valueOf(PropertyUtils.getProperty("proxy.retry.maxAttempts"));
        }
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.multiplier"))) {
            this.multiplier = Double.valueOf(PropertyUtils.getProperty("proxy.retry.multiplier"));
        }
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.initialwaitmsec"))) {
            this.initialWaitMsec = Long.valueOf(PropertyUtils.getProperty("proxy.retry.initialwaitmsec"));
        }
    }

    void setMagicAuthHeader() {
        MagicAuthenticationHeaderHttpRequestInterceptor authHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof MagicAuthenticationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        headers.put(Constants.INTERNAL_SERVICE_HEADERNAME, Constants.INTERNAL_SERVICE_HEADERVALUE);
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

    protected <T, B> T postKryo(final String method, final String url, final B body) {
        return postKryo(method, url, body, null);
    }

    protected <T, B> T postKryo(final String method, final String url, final B body, final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, false, true);
    }

    protected <T, B> T postWithoutLogBody(final String method, final String url, final B body, final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, false, false);
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, true, false);
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz,
            final boolean logBody, final boolean useKryo) {
        HttpMethod verb = HttpMethod.POST;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1, body, logBody);
                return exchange(url, verb, body, returnValueClazz, useKryo, useKryo).getBody();
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

    //FIXME: do not let object api return LedpException when it is a network issue, then this can be merge to normal post method
    @Deprecated
    protected <T, B> T postWithRetries(final String method, final String url, final B body,
            final Class<T> returnValueClazz) {
        HttpMethod verb = HttpMethod.POST;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1, body, true);
                return exchange(url, verb, body, returnValueClazz, false, true).getBody();
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
                logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
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
                logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
                String baseUrl = url.substring(0, url.indexOf("?"));
                String params = url.substring(url.indexOf("?") + 1);
                RestTemplate newRestTemplate = HttpClientUtils.newFormURLEncodedRestTemplate();
                return newRestTemplate.postForObject(baseUrl, params, returnValueClazz);
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
                logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
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

    protected void put(final String method, final String url) {
        put(method, url, null, false);
    }

    protected <B> void putWithoutLogBody(final String method, final String url, final B body) {
        put(method, url, body, false);
    }

    protected <B> void put(final String method, final String url, final B body) {
        put(method, url, body, true);
    }

    protected <B> void putKryo(final String method, final String url, final B body) {
        put(method, url, body, null, false, true);
    }

    protected <B> void put(final String method, final String url, final B body, boolean logBody) {
        put(method, url, body, null, logBody, false);
    }

    protected <B, T> T put(final String method, final String url, final B body, Class<T> returnClz) {
        return put(method, url, body, returnClz, false, false);
    }

    protected <B, T> T put(final String method, final String url, final B body, Class<T> returnClz, boolean logBody, boolean kryo) {
        HttpMethod verb = HttpMethod.PUT;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute((RetryCallback<T, RuntimeException>) context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1, body, logBody);
                ResponseEntity<T> response = exchange(url, verb, body, returnClz, kryo, false);
                if (returnClz == null || response == null) {
                    return null;
                } else {
                    return response.getBody();
                }
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

    protected <T> T patch(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, HttpMethod.PATCH, context.getRetryCount() + 1);
                return restTemplate.patchForObject(url, entity, returnValueClazz);
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
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1);
                return exchange(url, verb, null, returnValueClazz, false, false).getBody();
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
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1);
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

    protected <T> T getKryo(final String method, final String url, final Class<T> returnValueClazz) {
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                logInvocation(method, url, verb, context.getRetryCount() + 1);
                return exchange(url, verb, null, returnValueClazz, false, true).getBody();
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
                    logInvocation(method, url, HttpMethod.DELETE, context.getRetryCount() + 1);
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


    private <T, P> ResponseEntity<T> exchange(String url, HttpMethod method, P payload, Class<T> clz, //
                                              boolean kryoContent, boolean kryoResponse) {
        HttpHeaders headers = new HttpHeaders();

        if (clz != null) {
            // set headers for response
            if (kryoResponse) {
                headers.setAccept(Collections.singletonList(KryoHttpMessageConverter.KRYO));
            } else {
                headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN));
                headers.set(org.apache.http.HttpHeaders.ACCEPT_ENCODING, "gzip");
            }
        }
        if (payload != null) {
            // set headers for request
            if (kryoContent) {
                headers.setContentType(KryoHttpMessageConverter.KRYO);
            } else {
                headers.setContentType(MediaType.APPLICATION_JSON);
            }
        }
        if ((kryoContent || kryoResponse) && !headers.isEmpty()) {
            log.info("Headers: " + headers.toSingleValueMap());
        }
        HttpEntity<P> entity;
        if (payload == null) {
            entity = new HttpEntity<>(headers);
        } else {
            entity = new HttpEntity<>(payload, headers);
        }
        if (clz == null) {
            restTemplate.exchange(url, method, entity, Void.class);
            return ResponseEntity.ok(null);
        } else {
            return restTemplate.exchange(url, method, entity, clz);
        }
    }

    /**
     * Reactive api won't start fetching right away.
     * Data transmission starts when the flux is first time subscribed.
     * Retry should handle retry in callers, because it may be an infinite stream
     */
    protected <T> Flux<T> getFlux(String channel, String url, Class<T> clz) {
        WebClient.RequestHeadersSpec request = prepareReactiveRequest(url, HttpMethod.GET, null,false);
        Flux<T> flux = request.retrieve().bodyToFlux(clz);
        return appendLogInterceptors(flux, channel, url);
    }

    protected <K, V, P> Flux<Map<K, V>> postMapFlux(String channel, String url, P payload) {
        WebClient.RequestHeadersSpec request = prepareReactiveRequest(url, HttpMethod.POST, payload, false);
        Flux<Map<K, V>> flux = request.retrieve().bodyToFlux(new ParameterizedTypeReference<Map<K, V>>() {});
        return appendLogInterceptors(flux, channel, url);
    }

    protected <K, V, P> Mono<Map<K, V>> postMapMono(String channel, String url, P payload) {
        WebClient.RequestHeadersSpec request = prepareReactiveRequest(url, HttpMethod.POST, payload, true);
        Mono<Map<K, V>> mono = request.retrieve().bodyToMono(new ParameterizedTypeReference<Map<K, V>>() {});
        return appendLogInterceptors(mono, channel, url);
    }

    private <T> Flux<T> appendLogInterceptors(Flux<T> flux, String channel, String url) {
        return flux //
                .doOnSubscribe(subscription -> //
                        log.info(String.format("Start reading %s flux from %s", channel, url)))
                .doOnComplete(() ->
                        log.info(String.format("Finish reading %s flux from %s", channel, url)))
                .doOnCancel(() ->
                        log.info(String.format("Cancel reading %s flux from %s", channel, url)))
                .doOnError(throwable ->
                        log.error(String.format("Failed to read %s flux from %s", channel, url), throwable));
    }

    private <T> Mono<T> appendLogInterceptors(Mono<T> mono, String channel, String url) {
        //TODO: need to enhance error handling
        return mono //
                .doOnSubscribe(subscription -> //
                        log.info(String.format("Start reading %s mono from %s", channel, url)))
                .doOnCancel(() ->
                        log.info(String.format("Cancel reading %s mono from %s", channel, url)))
                .doOnError(throwable ->
                        log.error(String.format("Failed to read %s mono from %s", channel, url), throwable));
    }

    private <P> WebClient.RequestHeadersSpec prepareReactiveRequest(String url, HttpMethod method, P payload,
                                                                    boolean useKryo) {
        WebClient.RequestHeadersSpec request;

        if (payload != null) {
            request = webClient.method(method).uri(url).syncBody(payload);
        } else {
            request = webClient.method(method).uri(url);
        }

        for(Map.Entry<String, String> header: headers.entrySet()) {
            request = request.header(header.getKey(), header.getValue());
        }

        if (useKryo) {
            return request.accept(KryoHttpMessageConverter.KRYO);
        } else {
            return request.accept((MediaType.APPLICATION_JSON));
        }
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
        log.info("set " + getClass().getSimpleName() + " maxattemps to " + this.maxAttempts);
    }

    void enforceSSLNameVerification() {
        restTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
        cleanupAuthHeader();
    }

    private void logInvocation(String method, String url, HttpMethod verb, Integer attempt) {
        logInvocation(method, url, verb, attempt, null, false);
    }

    private <P> void logInvocation(String method, String url, HttpMethod verb, Integer attempt, P payload,
                                   boolean logPlayload) {
        String msg = String.format("Invoking %s by %s url %s", method, verb, url);
        if (logPlayload) {
            msg += String.format(" with body %s", payload == null ? "null" : payload.toString());
        }
        if (attempt != null) {
            msg += String.format(".  (Attempt=%d)", attempt);
        }
        log.info(msg);
    }

    private <K, V> BaseSubscriber<Map<K, V>> getMapSubscriber(String mode, String channel, String url) {
        return new BaseSubscriber<Map<K, V>>() {

            AtomicBoolean requested = new AtomicBoolean(false);

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                log.info(String.format("Start reading %s %s from %s", channel, mode, url));
                request(1);
            }

            @Override
            protected void hookOnComplete() {
                log.info(String.format("Complete reading %s %s from %s", channel, mode, url));
            }

            @Override
            protected void hookOnNext(Map<K, V> value) {
                request(1);
            }
        };
    }
}

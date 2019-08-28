package com.latticeengines.proxy.exposed;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriTemplate;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.proxy.framework.ErrorUtils;
import com.latticeengines.proxy.framework.ProxyRetryTemplate;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(BaseRestApiProxy.class);

    @SuppressWarnings("unchecked")
    private static final Set<Class<? extends Throwable>> DEFAULT_RETRY_EXCEPTIONS = ImmutableSet.of( //
            java.net.SocketTimeoutException.class, //
            java.sql.SQLTimeoutException.class, //
            com.mchange.v2.resourcepool.TimeoutException.class, //
            org.apache.commons.httpclient.util.TimeoutController.TimeoutException.class, //
            org.springframework.web.context.request.async.AsyncRequestTimeoutException.class, //
            io.netty.handler.timeout.TimeoutException.class, //
            org.apache.http.NoHttpResponseException.class,
            org.springframework.web.client.ResourceAccessException.class,
            java.io.IOException.class);

    private static final Set<String> DEFAULT_RETRY_MESSAGES = ImmutableSet.of("Connection reset");

    private RestTemplate restTemplate;
    private WebClient webClient;
    private String hostport;
    private String rootpath;
    private Set<Class<? extends Throwable>> retryExceptions;
    private Set<String> retryMessages;

    private long initialWaitMsec = 500;
    private double multiplier = 2D;
    private int maxAttempts = 5;

    private ConcurrentMap<String, String> headers = new ConcurrentHashMap<>();

    // Used to call external API because there is no standardized error handler
    protected BaseRestApiProxy() {
        this.restTemplate = HttpClientUtils.newRestTemplate();
        this.restTemplate.setErrorHandler(new GetResponseErrorHandler());
        this.webClient = HttpClientUtils.newWebClient();
        this.retryExceptions = DEFAULT_RETRY_EXCEPTIONS;
        this.retryMessages = DEFAULT_RETRY_MESSAGES;
        initialConfig();
    }

    protected BaseRestApiProxy(String hostport) {
        this(hostport, null);
    }

    protected BaseRestApiProxy(String hostport, String rootpath, Object... urlVariables) {
        this.hostport = hostport;
        this.rootpath = StringUtils.isEmpty(rootpath) ? ""
                : new UriTemplate(rootpath).expand(urlVariables).toString();
        this.restTemplate = HttpClientUtils.newRestTemplate();
        this.restTemplate.setErrorHandler(new GetResponseErrorHandler());
        this.webClient = HttpClientUtils.newWebClient();
        this.retryExceptions = DEFAULT_RETRY_EXCEPTIONS;
        this.retryMessages = DEFAULT_RETRY_MESSAGES;
        setMagicAuthHeader();
        initialConfig();
    }

    private void initialConfig() {
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.maxAttempts"))) {
            this.maxAttempts = Integer
                    .valueOf(PropertyUtils.getProperty("proxy.retry.maxAttempts"));
        }
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.multiplier"))) {
            this.multiplier = Double.valueOf(PropertyUtils.getProperty("proxy.retry.multiplier"));
        }
        if (StringUtils.isNotBlank(PropertyUtils.getProperty("proxy.retry.initialwaitmsec"))) {
            this.initialWaitMsec = Long
                    .valueOf(PropertyUtils.getProperty("proxy.retry.initialwaitmsec"));
        }
    }

    protected <B> ProxyRetryTemplate getRetryTemplate(final String method, HttpMethod verb,
            final String url, final boolean logBody, final B body) {
        ProxyRetryTemplate retryTemplate = new ProxyRetryTemplate(maxAttempts, initialWaitMsec,
                multiplier);
        retryTemplate.setRetryExceptions(retryExceptions);
        retryTemplate.setRetryMessages(retryMessages);
        retryTemplate.setMethod(method);
        retryTemplate.setVerb(verb);
        retryTemplate.setUrl(url);
        if (logBody) {
            retryTemplate.setBodyToLog(body);
        }
        return retryTemplate;
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

    protected String combine(Object... parts) {
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
                    part = part.substring(0, part.length() - 1);
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

    protected void setRetryMessages(final Set<String> retryMessages) {
        this.retryMessages = retryMessages;
    }

    void enforceSSLNameVerification() {
        restTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
        cleanupAuthHeader();
    }

    void setMagicAuthHeader() {
        MagicAuthenticationHeaderHttpRequestInterceptor authHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(
                restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof MagicAuthenticationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        headers.put(Constants.INTERNAL_SERVICE_HEADERNAME, Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(interceptors);
    }

    void setAuthHeader(String authToken) {
        AuthorizationHeaderHttpRequestInterceptor authHeader = new AuthorizationHeaderHttpRequestInterceptor(
                authToken);
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(
                restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    void setAuthInterceptor(AuthorizationHeaderHttpRequestInterceptor authHeader) {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(
                restTemplate.getInterceptors());
        interceptors.removeIf(i -> i instanceof AuthorizationHeaderHttpRequestInterceptor);
        interceptors.add(authHeader);
        restTemplate.setInterceptors(interceptors);
    }

    void cleanupAuthHeader() {
        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>(
                restTemplate.getInterceptors());
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

    protected <T, B> T postKryo(final String method, final String url, final B body,
            final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, true, true);
    }

    protected <B> void post(final String method, final String url, final B body) {
        post(method, url, body, Object.class, true, false);
    }

    protected <T, B> T post(final String method, final String url, final B body,
            final Class<T> returnValueClazz) {
        return post(method, url, body, returnValueClazz, true, false);
    }

    protected <T, B> T post(final String method, final String url, final B body,
            final Class<T> returnValueClazz, final boolean logBody, final boolean useKryo) {
        HttpMethod verb = HttpMethod.POST;
        RetryTemplate retry = getRetryTemplate(method, verb, url, logBody, body);
        return retry.execute(context -> {
            logInvocation(method, url, verb, context.getRetryCount() + 1, body, logBody);
            return exchange(url, verb, body, returnValueClazz, useKryo, useKryo).getBody();
        });
    }

    protected <T> T postMultiPart(final String method, final String url,
            final MultiValueMap<String, Object> parts, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.POST, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts,
                    headers);
            RestTemplate newRestTemplate = HttpClientUtils.newRestTemplate();
            List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
            for (ClientHttpRequestInterceptor interceptor : restTemplate.getInterceptors()) {
                if (interceptor instanceof AuthorizationHeaderHttpRequestInterceptor
                        || interceptor instanceof MagicAuthenticationHeaderHttpRequestInterceptor) {
                    interceptors.add(interceptor);
                }
            }
            newRestTemplate.setInterceptors(interceptors);
            ResponseEntity<T> response = newRestTemplate.exchange(url, HttpMethod.POST,
                    requestEntity, returnValueClazz);
            return response.getBody();
        });
    }

    protected <T> T postForUrlEncoded(final String method, final String url,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.POST, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
            String baseUrl = url.substring(0, url.indexOf("?"));
            String params = url.substring(url.indexOf("?") + 1);
            RestTemplate newRestTemplate = HttpClientUtils.newFormURLEncodedRestTemplate();
            return newRestTemplate.postForObject(baseUrl, params, returnValueClazz);
        });
    }

    protected <T> T postForEntity(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.POST, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, HttpMethod.POST, context.getRetryCount() + 1);
            return restTemplate.postForEntity(url, entity, returnValueClazz).getBody();
        });
    }

    protected void put(final String method, final String url) {
        put(method, url, null, false);
    }

    protected <B> void put(final String method, final String url, final B body) {
        put(method, url, body, false);
    }

    protected <B> void put(final String method, final String url, final B body, boolean logBody) {
        put(method, url, body, null, logBody, false);
    }

    protected <B, T> T put(final String method, final String url, final B body,
            Class<T> returnClz) {
        return put(method, url, body, returnClz, false, false);
    }

    protected <B, T> T put(final String method, final String url, final B body, Class<T> returnClz,
            boolean logBody, boolean kryo) {
        HttpMethod verb = HttpMethod.PUT;
        RetryTemplate retry = getRetryTemplate(method, verb, url, logBody, body);
        return retry.execute(context -> {
            logInvocation(method, url, verb, context.getRetryCount() + 1, body, logBody);
            ResponseEntity<T> response = exchange(url, verb, body, returnClz, kryo, false);
            if (returnClz == null || response == null) {
                return null;
            } else {
                return response.getBody();
            }
        });
    }

    protected <T> T patch(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.PATCH, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, HttpMethod.PATCH, context.getRetryCount() + 1);
            return restTemplate.patchForObject(url, entity, returnValueClazz);
        });
    }

    protected <T> T get(final String method, final String url, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate(method, verb, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, verb, context.getRetryCount() + 1);
            return restTemplate.exchange(url, verb, entity, returnValueClazz).getBody();
        });
    }

    protected <T> List<T> getList(final String method, final String url,
            final Class<T> returnValueClazz) {
        List<?> list = get(method, url, List.class, false);
        return JsonUtils.convertList(list, returnValueClazz);
    }

    protected <T> T get(final String method, final String url, final Class<T> returnValueClazz) {
        return get(method, url, returnValueClazz, false);
    }

    protected <T> T getKryo(final String method, final String url,
            final Class<T> returnValueClazz) {
        return get(method, url, returnValueClazz, true);
    }

    private <T> T get(final String method, final String url, final Class<T> returnValueClazz,
            boolean useKryo) {
        final HttpMethod verb = HttpMethod.GET;
        RetryTemplate retry = getRetryTemplate(method, verb, url, false, null);
        return retry.execute(context -> {
            logInvocation(method, url, verb, context.getRetryCount() + 1);
            return exchange(url, verb, null, returnValueClazz, false, useKryo).getBody();
        });
    }

    protected <T> T getGenericBinary(final String method, final String url, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.GET, url, false, null);
        return retry.execute(retryContext -> {
            logInvocation(method, url, HttpMethod.GET, retryContext.getRetryCount() + 1);
            ContentDisposition contentDisposition = ContentDisposition
                    .builder("attachment")
                    .filename("downloaded_file.csv")
                    .build();
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));
            headers.setContentDisposition(contentDisposition);
            HttpEntity<T> entity = new HttpEntity<>(headers);
            return restTemplate.exchange(url, HttpMethod.GET, entity, returnValueClazz).getBody();
        });
    }

    protected void delete(final String method, final String url) {
        RetryTemplate retry = getRetryTemplate(method, HttpMethod.DELETE, url, false, null);
        retry.execute((RetryCallback<Void, RuntimeException>) context -> {
            logInvocation(method, url, HttpMethod.DELETE, context.getRetryCount() + 1);
            restTemplate.delete(url);
            return null;
        });
    }

    <T, P> ResponseEntity<T> exchange(String url, HttpMethod method, P payload, Class<T> clz, //
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
     * Reactive api won't start fetching right away. Data transmission starts
     * when the flux is first time subscribed. Retry should handle retry in
     * callers, because it may be an infinite stream
     */
    protected <T> Mono<T> getMono(String channel, String url, Class<T> clz) {
        RequestHeadersSpec<?> request = prepareReactiveRequest(url, HttpMethod.GET, null, false);
        Mono<T> mono = request.retrieve().bodyToMono(clz);
        mono = appendMonoHandler(mono, channel);
        return appendLogInterceptors(mono, channel, url);
    }

    protected <T> Mono<T> getMonoKryo(String channel, String url, Class<T> clz) {
        WebClient.RequestHeadersSpec<?> request = prepareKryoReactiveRequest(url, HttpMethod.GET,
                null);
        Mono<T> mono = extractKryoMono(request, clz);
        mono = appendMonoHandler(mono, channel);
        return appendLogInterceptors(mono, channel, url);
    }

    protected <T> Flux<T> getFlux(String channel, String url, Class<T> clz) {
        RequestHeadersSpec<?> request = prepareReactiveRequest(url, HttpMethod.GET, null, false);
        Flux<T> flux = request.retrieve().bodyToFlux(clz);
        return appendLogInterceptors(flux, channel, url);
    }

    protected <T> Flux<T> getStream(String channel, String url, Class<T> clz) {
        RequestHeadersSpec<?> request = prepareReactiveRequest(url, HttpMethod.GET, null, true);
        Flux<T> flux = request.retrieve().bodyToFlux(clz);
        return appendLogInterceptors(flux, channel, url);
    }

    protected <T, P> Mono<T> postMono(String channel, String url, P payload, Class<T> clz) {
        RequestHeadersSpec<?> request = prepareReactiveRequest(url, HttpMethod.POST, payload,
                false);
        Mono<T> mono = request.retrieve().bodyToMono(clz);
        mono = appendMonoHandler(mono, channel);
        return appendLogInterceptors(mono, channel, url);
    }

    protected <T, P> Mono<T> postMonoKryo(String channel, String url, P payload, Class<T> clz) {
        RequestHeadersSpec<?> request = prepareKryoReactiveRequest(url, HttpMethod.POST, payload);
        Mono<T> mono = extractKryoMono(request, clz);
        mono = appendMonoHandler(mono, channel);
        return appendLogInterceptors(mono, channel, url);
    }

    protected <K, V, P> Mono<Map<K, V>> postMapMono(String channel, String url, P payload) {
        RequestHeadersSpec<?> request = prepareReactiveRequest(url, HttpMethod.POST, payload,
                false);
        Mono<Map<K, V>> mono = request.retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<K, V>>() {
                });
        mono = appendMonoHandler(mono, channel);
        return appendLogInterceptors(mono, channel, url);
    }

    private <T> Mono<T> extractKryoMono(WebClient.RequestHeadersSpec<?> request, Class<T> clz) {
        return request.retrieve().bodyToMono(ByteBuffer.class) //
                .map(byteBuffer -> KryoUtils.read(new ByteArrayInputStream(byteBuffer.array()),
                        clz));
    }

    private <T> Mono<T> appendMonoHandler(Mono<T> mono, String channel) {
        AtomicLong backoff = new AtomicLong(initialWaitMsec);
        return mono.onErrorResume((Throwable throwable) -> {
            if (throwable instanceof WebClientResponseException) {
                WebClientResponseException webException = (WebClientResponseException) throwable;
                return Mono.error(ErrorUtils.handleError(webException));
            } else {
                return Mono.error(throwable);
            }
        }).retryWhen(
                companion -> companion.zipWith(Flux.range(1, maxAttempts), (error, attempt) -> {
                    if (attempt < maxAttempts) {
                        String reason = ErrorUtils.shouldRetryFor(error, retryExceptions,
                                retryMessages);
                        if (StringUtils.isNotBlank(reason)) {
                            log.warn(String.format(
                                    "%s (Attempt=%d): Remote call failure, will retry: %s", channel, //
                                    attempt, reason));
                        } else {
                            attempt = maxAttempts;
                        }
                    }
                    if (attempt >= maxAttempts) {
                        throw Exceptions.propagate(error);
                    }
                    return attempt;
                }).flatMap(attempt -> {
                    Mono<Long> delay = Mono.delay(Duration.ofMillis(backoff.get()));
                    backoff.set((long) (backoff.get() * multiplier));
                    return delay;
                }));
    }

    private <T> Flux<T> appendLogInterceptors(Flux<T> flux, String channel, String url) {
        return flux //
                .doOnCancel(() -> log
                        .info(String.format("Cancel reading %s flux from %s", channel, url)))
                .onErrorResume(throwable -> {
                    log.warn(String.format("Failed to read %s flux from %s", channel, url));
                    return Flux.error(throwable);
                });
    }

    private <T> Mono<T> appendLogInterceptors(Mono<T> mono, String channel, String url) {
        return mono //
                .doOnCancel(() -> log
                        .info(String.format("Cancel reading %s mono from %s", channel, url)))
                .onErrorResume(throwable -> {
                    log.warn(String.format("Failed to read %s mono from %s", channel, url));
                    return Mono.error(throwable);
                });
    }

    @SuppressWarnings("rawtypes")
    private <P> WebClient.RequestHeadersSpec prepareKryoReactiveRequest(String url,
            HttpMethod method, P payload) {
        WebClient.RequestHeadersSpec request;

        if (payload != null) {
            request = webClient.method(method).uri(url).syncBody(payload);
        } else {
            request = webClient.method(method).uri(url);
        }

        for (Map.Entry<String, String> header : headers.entrySet()) {
            request = request.header(header.getKey(), header.getValue());
        }
        return request.accept(KryoHttpMessageConverter.KRYO);
    }

    @SuppressWarnings("rawtypes")
    private <P> WebClient.RequestHeadersSpec<?> prepareReactiveRequest(String url,
            HttpMethod method, P payload, boolean streaming) {
        WebClient.RequestHeadersSpec request;

        if (payload != null) {
            request = webClient.method(method).uri(url).syncBody(payload);
        } else {
            request = webClient.method(method).uri(url);
        }

        for (Map.Entry<String, String> header : headers.entrySet()) {
            request = request.header(header.getKey(), header.getValue());
        }

        if (streaming) {
            return request.accept(MediaType.APPLICATION_STREAM_JSON);
        } else {
            return request.accept(MediaType.APPLICATION_JSON);
        }
    }

    private void logInvocation(String method, String url, HttpMethod verb, Integer attempt) {
        logInvocation(method, url, verb, attempt, null, false);
    }

    private <P> void logInvocation(String method, String url, HttpMethod verb, Integer attempt,
            P payload, boolean logPlayload) {
        if (log.isDebugEnabled()) {
            String msg = String.format("Invoking %s by %s url %s", method, verb, url);
            if (logPlayload) {
                msg += String.format(" with body %s",
                        payload == null ? "null" : payload.toString());
            }
            if (attempt != null) {
                msg += String.format(".  (Attempt=%d)", attempt);
            }
            log.debug(msg);
        }
    }
}

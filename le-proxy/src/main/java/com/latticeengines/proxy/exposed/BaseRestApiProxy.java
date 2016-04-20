package com.latticeengines.proxy.exposed;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.serviceruntime.exposed.exception.GetResponseErrorHandler;

public abstract class BaseRestApiProxy {

    private RestTemplate restTemplate = new RestTemplate();
    private static final Log log = LogFactory.getLog(BaseRestApiProxy.class);
    private String rootpath;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    @Value("${proxy.quartz.rest.endpoint.hostport}")
    private String quartzHostPort;

    protected BaseRestApiProxy(String rootpath, Object... urlVariables) {
        this.rootpath = rootpath == null ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
    }

    public BaseRestApiProxy() {
        this(null);
    }

    protected <T, B> T post(String method, String url, B body, Class<T> returnValueClazz) {
        log.info(String.format("Invoking %s by posting to url %s with body %s", method, url, body));
        try {
            return restTemplate.postForObject(url, body, returnValueClazz);
        } catch (Exception e) {
            log.error(String.format("%s: Remote call failure", method), e);
            throw e;
        }
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz,
            RetryTemplate retryTemplate) {
        return retryTemplate.execute(new RetryCallback<T, RuntimeException>() {
            @Override
            public T doWithRetry(RetryContext context) {
                return post(method, url, body, returnValueClazz);
            }
        });
    }

    protected <B> void put(String method, String url, B body) {
        log.info(String.format("Invoking %s by putting to url %s with body %s", method, url, body));
        try {
            restTemplate.put(url, body);
        } catch (Exception e) {
            log.error(String.format("%s: Remote call failure", method), e);
            throw e;
        }
    }

    protected <B> void put(final String method, final String url, final B body, RetryTemplate retryTemplate) {
        retryTemplate.execute(new RetryCallback<Void, RuntimeException>() {
            @Override
            public Void doWithRetry(RetryContext context) {
                put(method, url, body);
                return null;
            }
        });
    }

    protected <T> T get(String method, String url, Class<T> returnValueClazz) {
        log.info(String.format("Invoking %s by getting from url %s", method, url));
        try {
            return restTemplate.getForObject(url, returnValueClazz);
        } catch (Exception e) {
            log.error(String.format("%s: Remote call failure", method), e);
            throw e;
        }
    }

    protected <T> T get(final String method, final String url, final Class<T> returnValueClazz,
            RetryTemplate retryTemplate) {
        return retryTemplate.execute(new RetryCallback<T, RuntimeException>() {

            @Override
            public T doWithRetry(RetryContext context) {
                return get(method, url, returnValueClazz);
            }
        });
    }

    protected void delete(String method, String url) {
        log.info(String.format("Invoking %s by deleting from url %s", method, url));
        try {
            restTemplate.delete(url);
        } catch (Exception e) {
            log.error(String.format("%s: Remote call failure", method), e);
            throw e;
        }
    }

    protected void delete(final String method, final String url, RetryTemplate retryTemplate) {
        retryTemplate.execute(new RetryCallback<Void, RuntimeException>() {
            @Override
            public Void doWithRetry(RetryContext context) {
                delete(method, url);
                return null;
            }
        });
    }

    protected RetryTemplate getRetryTemplate(long initialWaitMsec, int maxAttempts) {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxAttempts);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(2);
        retry.setBackOffPolicy(backOffPolicy);
        return retry;
    }

    protected String constructUrl() {
        return constructUrl(null);
    }

    protected String constructUrl(Object path, Object... urlVariables) {
        if (microserviceHostPort == null || microserviceHostPort.equals("")) {
            throw new NullPointerException("microserviceHostPort must be set");
        }
        String end = rootpath;
        if (path != null) {
            String expandedPath = new UriTemplate(path.toString()).expand(urlVariables).toString();
            end = combine(rootpath, expandedPath);
        }
        return combine(microserviceHostPort, end);
    }

    protected String constructQuartzUrl(Object path, Object... urlVariables) {
        if (StringUtils.isEmpty(quartzHostPort)) {
            throw new NullPointerException("quartzHostPort must be set");
        }
        String end = rootpath;
        if (path != null) {
            String expandedPath = new UriTemplate(path.toString()).expand(urlVariables).toString();
            end = combine(rootpath, expandedPath);
        }
        return combine(quartzHostPort, end);
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

    public String getMicroserviceHostPort() {
        return microserviceHostPort;
    }

    public void setMicroserviceHostPort(String microserviceHostPort) {
        this.microserviceHostPort = microserviceHostPort;
    }

    public String getQuartzHostPort() {
        return quartzHostPort;
    }

    public void setQuartzHostPort(String quartzHostPort) {
        this.quartzHostPort = quartzHostPort;
    }
}

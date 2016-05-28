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

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public abstract class CommonRestApiProxy {

    private RestTemplate restTemplate = new RestTemplate();
    private static final Log log = LogFactory.getLog(CommonRestApiProxy.class);
    protected String rootpath;

    @Value("${proxy.retry.initialwaitmsec:1000}")
    private long initialWaitMsec;

    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;

    @Value("${proxy.retry.maxAttempts:10}")
    private int maxAttempts;

    protected CommonRestApiProxy(String rootpath, Object... urlVariables) {
        this.rootpath = rootpath == null ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
    }

    public CommonRestApiProxy() {
        this(null);
    }

    protected <T, B> T post(final String method, final String url, final B body, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(new RetryCallback<T, RuntimeException>() {
            @Override
            public T doWithRetry(RetryContext context) throws RuntimeException {
                try {
                    log.info(String.format("Invoking %s by posting to url %s with body %s.  (Attempt=%d)", method, url,
                            body, context.getRetryCount() + 1));
                    return restTemplate.postForObject(url, body, returnValueClazz);
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

    private void logError(Exception e, String method) {
        log.error(String.format("%s: Remote call failure", method), e);
    }

    protected <B> void put(final String method, final String url, final B body) {
        RetryTemplate retry = getRetryTemplate();
        retry.execute(new RetryCallback<Void, RuntimeException>() {
            @Override
            public Void doWithRetry(RetryContext context) throws RuntimeException {
                try {
                    log.info(String.format("Invoking %s by putting to url %s with body %s.  (Attempt=%d)", method, url,
                            body, context.getRetryCount() + 1));
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

            }
        });
    }

    protected <T> T get(final String method, final String url, final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(new RetryCallback<T, RuntimeException>() {
            @Override
            public T doWithRetry(RetryContext context) {
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

    protected String constructUrl(String serviceHostPort) {
        return constructUrl(serviceHostPort, null);
    }

    protected String constructUrl(String serviceHostPort, Object path, Object... urlVariables) {
        if (serviceHostPort == null || serviceHostPort.equals("")) {
            throw new NullPointerException("serviceHostPort must be set");
        }
        String end = rootpath;
        if (path != null && urlVariables != null) {
            String expandedPath = new UriTemplate(path.toString()).expand(urlVariables).toString();
            end = combine(rootpath, expandedPath);
        }
        return combine(serviceHostPort, end);
    }

    protected String constructQuartzUrl(String quartzHostPort, Object path, Object... urlVariables) {
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
}

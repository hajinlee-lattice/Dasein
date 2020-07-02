package com.latticeengines.proxy.framework;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ProxyRetryTemplate extends RetryTemplate {

    private static final Logger log = LoggerFactory.getLogger(ProxyRetryTemplate.class);

    private Set<Class<? extends Throwable>> retryExceptions;
    private Set<String> retryMessages;
    private String method;
    private HttpMethod verb;
    private String url;
    private Object bodyToLog;
    private final int MAX_BODY_LOGGING_SIZE = 1000;

    public ProxyRetryTemplate(int maxAttempts, long initialWaitMsec, double multiplier) {
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxAttempts);
        this.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialWaitMsec);
        backOffPolicy.setMultiplier(multiplier);
        this.setBackOffPolicy(backOffPolicy);
        this.setThrowLastExceptionOnExhausted(true);
        this.addListener();
    }

    public void setRetryExceptions(Set<Class<? extends Throwable>> retryExceptions) {
        this.retryExceptions = retryExceptions;
    }

    public void setRetryMessages(Set<String> retryMessages) {
        this.retryMessages = retryMessages;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setVerb(HttpMethod verb) {
        this.verb = verb;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setBodyToLog(Object bodyToLog) {
        this.bodyToLog = bodyToLog;
    }

    private void addListener() {
        RetryListener retryListener = new RetryListener() {

            @Override
            public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
                return true;
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                    Throwable throwable) {
                if (throwable != null) {
                    if (bodyToLog != null) {
                        logError(method, url, verb, retryContext.getRetryCount(), bodyToLog, true);
                    } else {
                        logError(method, url, verb, retryContext.getRetryCount(), null, false);
                    }
                }
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                    Throwable throwable) {
                String reason = shouldRetryFor(throwable);
                if (StringUtils.isNotBlank(reason)) {
                    logWarn(reason, method, retryContext.getRetryCount());
                } else {
                    retryContext.setExhaustedOnly();
                }
            }
        };
        this.setListeners(new RetryListener[] { retryListener });
    }

    private String shouldRetryFor(Throwable e) {
        return ErrorUtils.shouldRetryFor(e, retryExceptions, retryMessages);
    }

    private void logWarn(String reason, String method, int attempt) {
        log.warn(String.format("%s (Attempt=%d): Remote call failure, will retry: %s", method, //
                attempt, reason));
    }

    private <P> void logError(String method, String url, HttpMethod verb, Integer attempt, P payload,
            boolean logPlayload) {
        String msg = String.format("Failed to invoke %s by %s url %s", method, verb, url);
        if (logPlayload) {
            msg += String.format(" with body %s", payload == null ? "null"
                    : StringUtils.truncate(JsonUtils.serialize(payload), MAX_BODY_LOGGING_SIZE));
        }
        if (attempt != null) {
            msg += String.format(".  (Attempt=%d)", attempt);
        }
        log.warn(msg);
    }

}

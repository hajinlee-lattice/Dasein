package com.latticeengines.lambda.authorizer.integ;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.lambda.authorizer.config.AuthorizerConfig;

public abstract class AbstractProvider {

    private static Logger LOG = Logger.getLogger(AbstractProvider.class);
    
    @Autowired
    protected AuthorizerConfig authorizerConfig;
    
    protected RestTemplate restTemplate = new RestTemplate();
    
    private RetryTemplate getRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(authorizerConfig.getRetryPolicy().getMaxAttemps());
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(authorizerConfig.getBackOffPolicy().getInitialInterval());
        backOffPolicy.setMultiplier(authorizerConfig.getBackOffPolicy().getMultiplier());
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }
    
    protected <T> T exchange(final String description, final String url, HttpMethod method, final HttpEntity<?> entity,
            final Class<T> returnValueClazz) {
        RetryTemplate retry = getRetryTemplate();
        return retry.execute(context -> {
            try {
                LOG.info(String.format("Invoking %s by getting from url %s with http headers.  (Attempt=%d)", method,
                        url, context.getRetryCount() + 1));
                return restTemplate.exchange(url, HttpMethod.GET, entity, returnValueClazz).getBody();
            } catch (Exception e) {
                logError(description, e);
                throw e;
            }
        });
    }

    private void logError(String description, Exception e) {
        LOG.error(description + " : " + e.getMessage());
    }
}

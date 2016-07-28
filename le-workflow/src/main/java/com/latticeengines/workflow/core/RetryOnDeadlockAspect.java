package com.latticeengines.workflow.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.hibernate.exception.LockAcquisitionException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
@Aspect
public class RetryOnDeadlockAspect {
    private static final Logger log = Logger.getLogger(RetryOnDeadlockAspect.class);

    private final int MAX_RETRIES = 10;
    private final int RETRY_WAIT_INITIAL_MSEC = 500;
    private final int RETRY_WAIT_MULTIPLIER = 2;

    @Around("execution(* org.springframework.batch.core.repository.JobRepository.*(..))")
    public Object retry(final ProceedingJoinPoint joinPoint) throws Throwable {
        RetryTemplate template = getRetryTemplate();
        return template.execute(new RetryCallback<Object, Throwable>() {
            @Override
            public Object doWithRetry(RetryContext context) throws Throwable {
                try {
                    return joinPoint.proceed();
                } catch (DeadlockLoserDataAccessException | DataAccessResourceFailureException
                        | LockAcquisitionException e) {
                    log.error(String.format("Deadlock detected performing method %s (attempt %d of %d)", joinPoint
                            .getSignature().getName(), context.getRetryCount() + 1, MAX_RETRIES), e);
                    throw e;
                }
            }
        });
    }

    private RetryTemplate getRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(DeadlockLoserDataAccessException.class, true);
        exceptionMap.put(DataAccessResourceFailureException.class, true);
        exceptionMap.put(LockAcquisitionException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(MAX_RETRIES, exceptionMap, true);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(RETRY_WAIT_INITIAL_MSEC);
        backOffPolicy.setMultiplier(RETRY_WAIT_MULTIPLIER);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }
}

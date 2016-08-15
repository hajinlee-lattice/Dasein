package com.latticeengines.common.exposed.retry;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.log4j.Logger;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

public class LoggingRetryOperationsInterceptor implements MethodInterceptor {
    private static final Logger log = Logger.getLogger(LoggingRetryOperationsInterceptor.class);

    private RetryOperations retryOperations = new RetryTemplate();

    public Object invoke(final MethodInvocation invocation) throws Throwable {

        RetryCallback<Object, Throwable> retryCallback = new RetryCallback<Object, Throwable>() {

            public Object doWithRetry(RetryContext context) throws Exception {

                if (invocation instanceof ProxyMethodInvocation) {
                    try {
                        return ((ProxyMethodInvocation) invocation).invocableClone().proceed();
                    } catch (Exception e) {
                        log.warn(String.format("Caught exception %s in retryable block (attempt %d) ", e.getMessage(),
                                context.getRetryCount() + 1), e);
                        throw e;
                    } catch (Error e) {
                        log.warn(String.format("Caught exception %s in retryable block (attempt %d) ", e.getMessage(),
                                context.getRetryCount() + 1), e);
                        throw e;
                    } catch (Throwable e) {
                        throw new IllegalStateException(e);
                    }
                } else {
                    throw new IllegalStateException(
                            "MethodInvocation of the wrong type detected - this should not happen with Spring AOP, "
                                    + "so please raise an issue if you see this exception");
                }
            }

        };

        return this.retryOperations.execute(retryCallback);
    }

    public void setRetryOperations(RetryOperations retryTemplate) {
        Assert.notNull(retryTemplate, "'retryOperations' cannot be null.");
        this.retryOperations = retryTemplate;
    }
}

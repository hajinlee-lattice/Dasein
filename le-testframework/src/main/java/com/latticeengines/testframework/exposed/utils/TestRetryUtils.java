package com.latticeengines.testframework.exposed.utils;

import java.util.Collections;
import java.util.concurrent.Callable;

import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.RetryUtils;

public class TestRetryUtils {

    public static <T> T retryForAssertionError(Callable<T> callable) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        try {
            return retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    Thread.sleep(1000);
                }
                return callable.call();
            });
        } catch (RuntimeException|AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void retryForAssertionError(Runnable runnable) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        try {
            retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    Thread.sleep(1000);
                }
                runnable.run();
                return 1;
            });
        } catch (RuntimeException|AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

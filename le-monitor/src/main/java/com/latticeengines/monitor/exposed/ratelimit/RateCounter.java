package com.latticeengines.monitor.exposed.ratelimit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.exception.LedpCode;

public abstract class RateCounter {
    private Lock lock = new ReentrantLock();

    private AtomicInteger recordsCounter = new AtomicInteger();

    protected abstract int getRatelimit();

    public abstract int getRecordCount(Object[] args);

    public void shouldAccept(HttpServletRequest request, int size) {
        boolean shouldAccept = false;
        try {
            if (recordsCounter.get() < getRatelimit()) {
                lock.lock();
                if (recordsCounter.get() < getRatelimit()) {
                    if (recordsCounter.get() + size < getRatelimit()) {
                        shouldAccept = true;
                        recordsCounter.set(recordsCounter.get() + size);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        if (!shouldAccept) {
            throw new RateLimitException(LedpCode.LEDP_00005, //
                    new String[] { //
                            request.getRequestURI() },
                    request.getRequestURI());
        }
    }

    public void decrementCounter(int size) {
        try {
            lock.lock();
            recordsCounter.set(recordsCounter.get() - size);
        } finally {
            lock.unlock();
        }
    }

    public HttpServletRequest getHttpRequest(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                return (HttpServletRequest) arg;
            }
        }

        return null;
    }
}

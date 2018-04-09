package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadPoolUtils {

    private static long TIMEOUT = TimeUnit.HOURS.toMillis(1);

    public static ExecutorService getFixedSizeThreadPool(String name, int size) {
        ThreadFactory threadFac = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
        return Executors.newFixedThreadPool(size, threadFac);
    }

    public static ExecutorService getCachedThreadPool(String name) {
        ThreadFactory threadFac = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
        return Executors.newCachedThreadPool(threadFac);
    }

    public static ForkJoinPool getForkJoinThreadPool(String name, int size) {
        // custom workerThreadFactory for ensuring specified thread name prefix
        ForkJoinWorkerThreadFactory workerThreadFactory = //
                pool -> {
                    ForkJoinWorkerThread thread = //
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName(String.format("%s-%d", name, thread.getPoolIndex()));
                    return thread;
                };
        return new ForkJoinPool(size, workerThreadFactory, null, false);
    }

    public static <T> List<T> runOnThreadPool(ExecutorService executorService,
                                              List<Callable<T>> callables) {
        List<Future<T>> futures = callables.stream().map(executorService::submit).collect(Collectors.toList());
        List<T> results = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        while (CollectionUtils.isNotEmpty(futures)) {
            if (System.currentTimeMillis() - startTime > TIMEOUT) {
                throw new RuntimeException("Cannot finish all callables within timeout.");
            }
            List<Future> toBeRemoved = new ArrayList<>();
            futures.forEach(future -> {
                try {
                    T result = future.get(1, TimeUnit.SECONDS);
                    results.add(result);
                    toBeRemoved.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (InterruptedException|ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            toBeRemoved.forEach(futures::remove);
        }
        return results;
    }

}

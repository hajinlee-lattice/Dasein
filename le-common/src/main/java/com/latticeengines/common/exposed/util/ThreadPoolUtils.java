package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadPoolUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadPoolUtils.class);
    private static final String DEBUG_GATEWAY = "DebugGateway";

    public static ExecutorService getFixedSizeThreadPool(String name, int size) {
        ThreadFactory threadFac = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
        ExecutorService executorService = Executors.newFixedThreadPool(size, threadFac);
        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdownNow));
        return executorService;
    }

    public static ExecutorService getCachedThreadPool(String name) {
        ThreadFactory threadFac = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
        ExecutorService executorService = Executors.newCachedThreadPool(threadFac);
        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdownNow));
        return executorService;
    }

    public static ForkJoinPool getForkJoinThreadPool(String name) {
        ForkJoinPool pool = getForkJoinThreadPool(name, null);
        Runtime.getRuntime().addShutdownHook(new Thread(pool::shutdownNow));
        return pool;
    }

    public static ForkJoinPool getForkJoinThreadPool(String name, Integer size) {
        // custom workerThreadFactory for ensuring specified thread name prefix
        ForkJoinWorkerThreadFactory workerThreadFactory = //
                pool -> {
                    ForkJoinWorkerThread thread = //
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName(String.format("%s-%d", name, thread.getPoolIndex()));
                    return thread;
                };
        size = size == null ? Runtime.getRuntime().availableProcessors() : size;
        return new ForkJoinPool(size, workerThreadFactory, null, false);
    }

    public static <T> List<T> runCallablesInParallel(ExecutorService executorService, List<Callable<T>> callables,
            int timeoutInMinutes, int intervalInSeconds) {
        if (CollectionUtils.isNotEmpty(callables)) {
            int numTasks = CollectionUtils.size(callables);
            List<Callable<T>> wrappedCallables = callables.stream() //
                    .map(ThreadPoolUtils::wrapForDebugGateway).collect(Collectors.toList());
            List<Future<T>> futures = wrappedCallables.stream().map(executorService::submit) //
                    .collect(Collectors.toList());
            List<T> results = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            long timeout = TimeUnit.MINUTES.toMillis(timeoutInMinutes);
            while (CollectionUtils.isNotEmpty(futures)) {
                if (System.currentTimeMillis() - startTime > timeout) {
                    throw new RuntimeException("Cannot finish all callables within timeout.");
                }
                List<Future<T>> toBeRemoved = new ArrayList<>();
                futures.forEach(future -> {
                    try {
                        T result = future.get(intervalInSeconds, TimeUnit.SECONDS);
                        results.add(result);
                        toBeRemoved.add(future);
                    } catch (TimeoutException e) {
                        // ignore
                    } catch (InterruptedException | ExecutionException e) {
                        toBeRemoved.add(future);
                        throw new RuntimeException(e);
                    }
                });
                toBeRemoved.forEach(futures::remove);
            }
            double duration = (System.currentTimeMillis() - startTime) * 0.001;
            log.debug("Finished all of " + numTasks + " callable futures in " + duration + " sec.");
            return results;
        } else {
            log.warn("Empty callables are submitted, skip execution and return empty list.");
            return Collections.emptyList();
        }
    }

    public static <T extends Runnable> void runRunnablesInParallel(ExecutorService executorService, List<T> runnables,
            int timeoutInMinutes, int intervalInSeconds) {
        if (CollectionUtils.isNotEmpty(runnables)) {
            int numTasks = CollectionUtils.size(runnables);
            List<Runnable> wrappedRunnables = runnables.stream() //
                    .map(ThreadPoolUtils::wrapForDebugGateway).collect(Collectors.toList());
            List<Future<?>> futures = wrappedRunnables.stream() //
                    .map(executorService::submit).collect(Collectors.toList());
            long startTime = System.currentTimeMillis();
            long timeout = TimeUnit.MINUTES.toMillis(timeoutInMinutes);
            while (CollectionUtils.isNotEmpty(futures)) {
                if (System.currentTimeMillis() - startTime > timeout) {
                    throw new RuntimeException("Cannot finish all runnables within timeout.");
                }
                List<Future<?>> toBeRemoved = new ArrayList<>();
                futures.forEach(future -> {
                    try {
                        future.get(intervalInSeconds, TimeUnit.SECONDS);
                        toBeRemoved.add(future);
                    } catch (TimeoutException e) {
                        // ignore
                    } catch (InterruptedException | ExecutionException e) {
                        toBeRemoved.add(future);
                        throw new RuntimeException(e);
                    }
                });
                toBeRemoved.forEach(futures::remove);
            }
            double duration = (System.currentTimeMillis() - startTime) * 0.001;
            log.debug("Finished all of " + numTasks + " runnable futures in " + duration + " sec.");
        } else {
            log.warn("Empty runnables are submitted, skip execution.");
        }
    }

    public static void shutdownAndAwaitTermination(ExecutorService pool, long threadPoolTimeoutMin) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(threadPoolTimeoutMin, TimeUnit.MINUTES)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(threadPoolTimeoutMin, TimeUnit.MINUTES))
                    log.warn("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public static void shutdownAndAwaitTermination(ExecutorService pool) {
        shutdownAndAwaitTermination(pool, 1);
    }

    public static Runnable wrapForDebugGateway(Runnable runnable) {
        if (ThreadContext.containsKey(DEBUG_GATEWAY)) {
            return () -> {
                ThreadContext.put(DEBUG_GATEWAY, "ON");
                try {
                    runnable.run();
                } finally {
                    ThreadContext.remove(DEBUG_GATEWAY);
                }
            };
        } else {
            return runnable;
        }
    }

    public static <T> Callable<T> wrapForDebugGateway(Callable<T> callable) {
        if (ThreadContext.containsKey(DEBUG_GATEWAY)) {
            return () -> {
                ThreadContext.put(DEBUG_GATEWAY, "ON");
                try {
                    return callable.call();
                } finally {
                    ThreadContext.remove(DEBUG_GATEWAY);
                }
            };
        } else {
            return callable;
        }
    }

}

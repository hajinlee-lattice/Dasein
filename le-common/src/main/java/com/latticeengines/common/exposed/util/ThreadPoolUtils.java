package com.latticeengines.common.exposed.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadPoolUtils {

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

}

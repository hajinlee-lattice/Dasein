package com.latticeengines.common.exposed.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import reactor.core.publisher.Flux;

public class ThreadPoolUtilsUnitTestNG {


    @SuppressWarnings("rawtypes")
    @Test(groups = "unit")
    private void testForkJoinPool() {
        AtomicLong counter = new AtomicLong(0L);
        ForkJoinPool pool = ThreadPoolUtils.getForkJoinThreadPool("test", 4);
        List<Runnable> runnables = getRunnables(counter);
        List<ForkJoinTask> tasks = runnables.stream().map(pool::submit).collect(Collectors.toList());
        tasks.forEach(ForkJoinTask::join);
        Assert.assertEquals(counter.get(), 100L);
    }

    @Test(groups = "unit")
    private void testParallelRunnables() {
        AtomicLong counter = new AtomicLong(0L);
        ForkJoinPool pool = ThreadPoolUtils.getForkJoinThreadPool("test", 4);
        List<Runnable> runnables = getRunnables(counter);
        ThreadPoolUtils.runInParallel(pool, runnables, 1, 1);
        Assert.assertEquals(counter.get(), 100L);
    }

    @Test(groups = "unit")
    private void testBoundedQueueCallerThreadPool() throws InterruptedException {
        AtomicLong counter = new AtomicLong(0L);
        ExecutorService pool = ThreadPoolUtils.getBoundedQueueCallerThreadPool(1, 4, 1, 2);
        List<Runnable> runnables = getRunnables(counter);
        for (Runnable run : runnables) {
            pool.submit(run);
        }
        ThreadPoolUtils.shutdownAndAwaitTermination(pool, 1);
    }

    private List<Runnable> getRunnables(AtomicLong counter) {
        return Flux.range(0, 100).map(i -> (Runnable) () -> {
            counter.incrementAndGet();
            String thread = Thread.currentThread().getName();
            System.out.println(thread + ": " + i);
        }).collectList().block();
    }

}

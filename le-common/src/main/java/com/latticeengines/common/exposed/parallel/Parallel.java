package com.latticeengines.common.exposed.parallel;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Parallel {
    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    public static <T> void foreach(final Iterable<T> elements, final Operation<T> operation) {
        ExecutorService forPool = Executors.newFixedThreadPool(NUM_CORES * 2);

        try {
            forPool.invokeAll(createCallables(elements, operation));
        } catch (InterruptedException e) {
            // pass
        }
    }

    private static <T> Collection<Callable<Void>> createCallables(final Iterable<T> elements,
            final Operation<T> operation) {
        List<Callable<Void>> callables = new LinkedList<>();
        for (final T elem : elements) {
            callables.add(new Callable<Void>() {
                @Override
                public Void call() {
                    operation.perform(elem);
                    return null;
                }
            });
        }

        return callables;
    }

    public interface Operation<T> {
        void perform(T parameter);
    }
}

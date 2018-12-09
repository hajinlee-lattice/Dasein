package com.latticeengines.domain.exposed.metadata.mds;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class DecoratorChain implements Decorator, NeedsLoad {

    private final Iterable<Decorator> decorators;
    private String name;
    private AtomicBoolean loaded = new AtomicBoolean();
    private static ExecutorService DC_LOADERS;

    public DecoratorChain(String name, Decorator... decorators) {
        this.name = name;
        this.decorators = ImmutableList.copyOf(decorators);
    }

    DecoratorChain(String name, Iterable<Decorator> decorators) {
        this.name = name;
        this.decorators = ImmutableList.copyOf(decorators);
    }

    @Override
    public boolean isLoaded() {
        return loaded.get();
    }

    @Override
    public void load() {
        List<Runnable> runnables = new ArrayList<>();
        for (Decorator decorator: decorators) {
            if (decorator instanceof NeedsLoad) {
                runnables.add(((NeedsLoad) decorator)::load);
            }
        }
        if (CollectionUtils.isNotEmpty(runnables)) {
            ThreadPoolUtils.runRunnablesInParallel(getDcLoaders(), runnables, 60, 1);
        }
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        if (metadata == null) {
            throw new NullPointerException("Must specify metadata flux.");
        }
        Flux<ColumnMetadata> output = metadata.doOnSubscribe(s -> blockingLoad());
        for (Decorator decorator : decorators) {
            output = decorator.render(output);
        }
        return output;
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        if (metadata == null) {
            throw new NullPointerException("Must specify metadata flux.");
        }
        ParallelFlux<ColumnMetadata> output = metadata.doOnSubscribe(s -> blockingLoad());
        for (Decorator decorator : decorators) {
            output = decorator.render(output);
        }
        return output;
    }

    @Override
    public String getName() {
        return name;
    }

    private static ExecutorService getDcLoaders() {
        if (DC_LOADERS == null) {
            synchronized (DecoratorChain.class) {
                if (DC_LOADERS == null) {
                    DC_LOADERS = ThreadPoolUtils.getCachedThreadPool("decorator-chain");
                }
            }
        }
        return DC_LOADERS;
    }

}

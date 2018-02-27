package com.latticeengines.domain.exposed.metadata.mds;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

public class DecoratorChain implements Decorator, NeedsLoad {

    private String name;
    private final Iterable<Decorator> decorators;

    private AtomicBoolean loaded = new AtomicBoolean();

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
    public Mono<Boolean> load() {
        return Flux.fromIterable(decorators) //
                .doOnSubscribe(s -> loaded.set(true)) //
                .parallel().runOn(ThreadPoolUtils.getMdsScheduler()) //
                .filter(d -> d instanceof NeedsLoad) //
                .map(d -> (NeedsLoad) d) //
                .flatMap(d -> {
                    if (d.isLoaded()) {
                        return Mono.just(true);
                    } else {
                        return d.load();
                    }
                }) //
                .sequential().last();
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        if (metadata == null) {
            throw new NullPointerException("Must specify metadata flux.");
        }
        Flux<ColumnMetadata> output = metadata.doOnSubscribe(s -> blockingLoad());
        for (Decorator decorator: decorators) {
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
        for (Decorator decorator: decorators) {
            output = decorator.render(output);
        }
        return output;
    }

    @Override
    public String getName() {
        return name;
    }

}

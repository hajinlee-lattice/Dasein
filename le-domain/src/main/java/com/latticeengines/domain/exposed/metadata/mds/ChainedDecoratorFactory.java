package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;

public abstract class ChainedDecoratorFactory<N extends Namespace> implements DecoratorFactory<N> {

    private final String decoratorName;
    private final Iterable<DecoratorFactory> factories;

    public ChainedDecoratorFactory(String decoratorName, Iterable<DecoratorFactory> factories) {
        this.decoratorName = decoratorName;
        this.factories = factories;
    }

    @Override
    public Decorator getDecorator(N namespace) {
        @SuppressWarnings("unchecked")
        Iterable<Decorator> decorators = Flux.fromIterable(factories)
                .zipWith(Flux.fromIterable(project(namespace)))
                .map(tuple2 -> tuple2.getT1().getDecorator(tuple2.getT2()))
                .toIterable();
        return new DecoratorChain(decoratorName, decorators);
    }

    protected abstract Iterable<Namespace> project(N namespace);

}

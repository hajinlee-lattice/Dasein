package com.latticeengines.domain.exposed.metadata.mds;

import java.util.List;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;

public abstract class ChainedDecoratorFactory<N extends Namespace> implements DecoratorFactory<N> {

    private final String decoratorName;
    private final List<DecoratorFactory<Namespace>> factories;

    @SuppressWarnings("unchecked")
    protected ChainedDecoratorFactory(String decoratorName,
            List<DecoratorFactory<? extends Namespace>> factories) {
        this.decoratorName = decoratorName;
        this.factories = factories.stream().map(factory -> (DecoratorFactory<Namespace>) factory) //
                .collect(Collectors.toList());
    }

    @Override
    public Decorator getDecorator(N namespace) {
        Iterable<Decorator> decorators = Flux.fromIterable(factories)
                .zipWith(Flux.fromIterable(project(namespace)))
                .map(tuple2 -> tuple2.getT1().getDecorator(tuple2.getT2())).toIterable();
        return new DecoratorChain(decoratorName, decorators);
    }

    protected abstract List<Namespace> project(N namespace);

}

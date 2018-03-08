package com.latticeengines.domain.exposed.metadata.mds;

import java.util.Collection;
import java.util.function.Function;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;

public class MdsDecoratorFactory<N extends Namespace> implements DecoratorFactory<N> {

    private final String decoratorName;
    private final MetadataStore<N> metadataStore;
    private final Function<ColumnMetadata, ColumnMetadata> propertyFilter;

    public static <N extends Namespace> MdsDecoratorFactory<N> fromMds(String decoratorName,
            MetadataStore<N> metadataStore) {
        return new MdsDecoratorFactory<>(decoratorName, metadataStore, null);
    }

    public static <N extends Namespace> MdsDecoratorFactory<N> fromMds(String decoratorName,
            MetadataStore<N> metadataStore, Function<ColumnMetadata, ColumnMetadata> propertyFilter) {
        return new MdsDecoratorFactory<>(decoratorName, metadataStore, propertyFilter);
    }

    private MdsDecoratorFactory(String decoratorName, MetadataStore<N> metadataStore,
            Function<ColumnMetadata, ColumnMetadata> propertyFilter) {
        this.decoratorName = decoratorName;
        this.metadataStore = metadataStore;
        this.propertyFilter = propertyFilter;
    }

    @Override
    public Decorator getDecorator(N namespace) {
        return new MapDecorator(decoratorName) {
            @Override
            protected Collection<ColumnMetadata> loadInternal() {
                Flux<ColumnMetadata> flux = metadataStore.getMetadata(namespace);
                if (propertyFilter != null) {
                    flux = flux.map(propertyFilter);
                }
                return flux.collectList().block();
            }
        };
    }

}

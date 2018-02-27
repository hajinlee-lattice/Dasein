package com.latticeengines.domain.exposed.metadata.mds;

import java.util.Collection;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

public class MdsDecoratorFactory<N extends Namespace> implements DecoratorFactory<N> {

    private final String decoratorName;
    private final MetadataStore<N> metadataStore;

    public static <N extends Namespace> MdsDecoratorFactory<N> fromMds(String decoratorName,
            MetadataStore<N> metadataStore) {
        return new MdsDecoratorFactory<>(decoratorName, metadataStore);
    }

    private MdsDecoratorFactory(String decoratorName, MetadataStore<N> metadataStore) {
        this.decoratorName = decoratorName;
        this.metadataStore = metadataStore;
    }

    @Override
    public Decorator getDecorator(N namespace) {
        return new MapDecorator(decoratorName) {
            @Override
            protected Collection<ColumnMetadata> loadInternal() {
                return metadataStore.getMetadata(namespace).collectList().block();
            }
        };
    }

}

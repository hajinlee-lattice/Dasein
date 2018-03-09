package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public abstract class DecoratedMetadataStore <N extends Namespace, BaseNS extends Namespace, DecoratorNS extends Namespace>
        implements MetadataStore<N> {

    private final MetadataStore<BaseNS> kernel;
    private final DecoratorFactory<DecoratorNS> factory;

    public DecoratedMetadataStore(MetadataStore<BaseNS> base, DecoratorFactory<DecoratorNS> factory) {
        this.kernel = base;
        this.factory = factory;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(N namespace) {
        DecoratorNS metadataNamespace = projectDecoratorNamespace(namespace);
        BaseNS dataNamespace = projectBaseNamespace(namespace);
        return factory.getDecorator(metadataNamespace).render(kernel.getMetadata(dataNamespace));
    }

    @Override
    public ParallelFlux<ColumnMetadata> getMetadataInParallel(N namespace) {
        DecoratorNS metadataNamespace = projectDecoratorNamespace(namespace);
        BaseNS dataNamespace = projectBaseNamespace(namespace);
        return factory.getDecorator(metadataNamespace).render(kernel.getMetadataInParallel(dataNamespace));
    }

    protected abstract DecoratorNS projectDecoratorNamespace(N namespace);

    protected abstract BaseNS projectBaseNamespace(N namespace);

}

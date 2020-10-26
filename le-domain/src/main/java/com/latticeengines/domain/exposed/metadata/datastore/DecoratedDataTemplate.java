package com.latticeengines.domain.exposed.metadata.datastore;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public abstract class DecoratedDataTemplate<N extends Namespace, BaseNS extends Namespace, DecoratorNS extends Namespace>
        implements DataUnitStore<N> {

    private final DataUnitStore<BaseNS> kernel;
    private final DecoratorFactory<DecoratorNS> factory;

    public DecoratedDataTemplate(DataUnitStore<BaseNS> base, DecoratorFactory<DecoratorNS> factory) {
        this.kernel = base;
        this.factory = factory;
    }

    @Override
    public List<DataUnit> getData(N namespace) {
        BaseNS dataNamespace = projectBaseNamespace(namespace);
        return kernel.getData(dataNamespace);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(N namespace) {
        DecoratorNS metadataNamespace = projectDecoratorNamespace(namespace);
        BaseNS dataNamespace = projectBaseNamespace(namespace);
        return factory.getDecorator(metadataNamespace).render(kernel.getSchema(dataNamespace));
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(N namespace) {
        DecoratorNS metadataNamespace = projectDecoratorNamespace(namespace);
        BaseNS dataNamespace = projectBaseNamespace(namespace);
        return factory.getDecorator(metadataNamespace)
                .render(kernel.getUnorderedSchema(dataNamespace));
    }

    protected abstract DecoratorNS projectDecoratorNamespace(N namespace);

    protected abstract BaseNS projectBaseNamespace(N namespace);

}

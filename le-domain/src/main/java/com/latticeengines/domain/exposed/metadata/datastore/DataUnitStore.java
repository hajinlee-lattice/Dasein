package com.latticeengines.domain.exposed.metadata.datastore;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface DataUnitStore<N extends Namespace> extends MetadataStore<N> {

    List<DataUnit> getData(N namespace);

    Flux<ColumnMetadata> getSchema(N namespace);

    ParallelFlux<ColumnMetadata> getUnorderedSchema(N namespace);

    @Override
    default Flux<ColumnMetadata> getMetadata(N namespace) {
        return getSchema(namespace);
    }

    @Override
    default ParallelFlux<ColumnMetadata> getMetadataInParallel(N namespace) {
        return getUnorderedSchema(namespace);
    }
}

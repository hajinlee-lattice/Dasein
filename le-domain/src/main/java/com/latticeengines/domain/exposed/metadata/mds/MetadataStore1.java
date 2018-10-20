package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface MetadataStore1<T extends Serializable> extends MetadataStore<Namespace1<T>> {

    default Flux<ColumnMetadata> getMetadata(T coord1) {
        return getMetadata(Namespace.as(coord1));
    }

    default ParallelFlux<ColumnMetadata> getMetadataInParallel(T coord1) {
        return getMetadataInParallel(Namespace.as(coord1));
    }

}

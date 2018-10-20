package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface MetadataStore2<T1 extends Serializable, T2 extends Serializable>
        extends MetadataStore<Namespace2<T1, T2>> {

    default Flux<ColumnMetadata> getMetadata(T1 coord1, T2 coord2) {
        return getMetadata(Namespace.as(coord1, coord2));
    }

    default ParallelFlux<ColumnMetadata> getMetadataInParallel(T1 coord1, T2 coord2) {
        return getMetadataInParallel(Namespace.as(coord1, coord2));
    }

}

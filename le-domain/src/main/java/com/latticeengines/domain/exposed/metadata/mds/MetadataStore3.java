package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface MetadataStore3<T1 extends Serializable, T2 extends Serializable, T3 extends Serializable>
        extends MetadataStore<Namespace3<T1, T2, T3>> {

    default Flux<ColumnMetadata> getMetadata(T1 coord1, T2 coord2, T3 coord3) {
        return getMetadata(Namespace.as(coord1, coord2, coord3));
    }

    default ParallelFlux<ColumnMetadata> getMetadataInParallel(T1 coord1, T2 coord2, T3 coord3) {
        return getMetadataInParallel(Namespace.as(coord1, coord2, coord3));
    }
}

package com.latticeengines.domain.exposed.metadata.datastore;

import java.io.Serializable;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface DataUnitStore2<T1 extends Serializable, T2 extends Serializable>
        extends DataUnitStore<Namespace2<T1, T2>> {

    default List<DataUnit> getData(T1 coord1, T2 coord2) {
        return getData(Namespace.as(coord1, coord2));
    }

    default Flux<ColumnMetadata> getSchema(T1 coord1, T2 coord2) {
        return getSchema(Namespace.as(coord1, coord2));

    }

    default ParallelFlux<ColumnMetadata> getUnorderedSchema(T1 coord1, T2 coord2) {
        return getUnorderedSchema(Namespace.as(coord1, coord2));
    }

}

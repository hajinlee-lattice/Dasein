package com.latticeengines.domain.exposed.metadata.datatemplate;

import java.io.Serializable;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface DataTemplate1<T extends Serializable> extends DataTemplate<Namespace1<T>> {

    default List<DataUnit> getData(T coord) {
        return getData(Namespace.as(coord));
    }

    default Flux<ColumnMetadata> getSchema(T coord) {
        return getSchema(Namespace.as(coord));

    }

    default ParallelFlux<ColumnMetadata> getUnorderedSchema(T coord) {
        return getUnorderedSchema(Namespace.as(coord));
    }

}

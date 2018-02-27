package com.latticeengines.domain.exposed.metadata.datatemplate;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface DataTemplate<N extends Namespace> {

    List<DataUnit> getData(N namespace);

    Flux<ColumnMetadata> getSchema(N namespace);

    ParallelFlux<ColumnMetadata> getUnorderedSchema(N namespace);

}


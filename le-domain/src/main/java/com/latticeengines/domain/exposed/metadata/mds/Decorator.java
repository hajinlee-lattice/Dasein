package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Named;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface Decorator extends Named {

    // enrich/overwrite metadata in the input schema
    Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata);
    ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata);

}

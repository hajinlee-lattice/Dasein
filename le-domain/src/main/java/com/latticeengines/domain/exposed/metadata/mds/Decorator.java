package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Named;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace0;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface Decorator extends Named, DecoratorFactory<Namespace0> {

    // enrich/overwrite metadata in the input schema
    Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata);
    ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata);

    @Override
    default Decorator getDecorator(Namespace0 namespace0) {
        return this;
    }

}

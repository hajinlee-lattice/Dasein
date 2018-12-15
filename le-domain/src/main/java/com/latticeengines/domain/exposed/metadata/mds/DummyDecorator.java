package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class DummyDecorator implements Decorator {


    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata;
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata;
    }

    @Override
    public String getName() {
        return "dummy";
    }
}

package com.latticeengines.apps.cdl.mds.impl;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class APSAttrsDecorator implements Decorator {

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        // TODO Auto-generated method stub
        return null;
    }

}

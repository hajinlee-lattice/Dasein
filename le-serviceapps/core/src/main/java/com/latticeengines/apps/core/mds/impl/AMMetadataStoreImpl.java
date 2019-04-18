package com.latticeengines.apps.core.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component
public class AMMetadataStoreImpl implements AMMetadataStore {

    private final ColumnMetadataProxy columnMetadataProxy;
    private static final Scheduler scheduler = Schedulers.newParallel("am-metadata");

    @Inject
    public AMMetadataStoreImpl(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<String> namespace) {
        String dcVersion = namespace.getCoord1();
        return getRawFlux(dcVersion) //
                .map(ColumnMetadata::clone); // column metadata proxy cache should be read only
    }

    @Override
    public ParallelFlux<ColumnMetadata> getMetadataInParallel(Namespace1<String> namespace) {
        String dcVersion = namespace.getCoord1();
        return getRawFlux(dcVersion) //
                .parallel().runOn(scheduler) //
                .map(ColumnMetadata::clone); // column metadata proxy cache should be read only
    }

    private Flux<ColumnMetadata> getRawFlux(String dcVersion) {
        return Mono.fromCallable(() -> columnMetadataProxy.getAllColumns(dcVersion)).flatMapMany(Flux::fromIterable);
    }

}

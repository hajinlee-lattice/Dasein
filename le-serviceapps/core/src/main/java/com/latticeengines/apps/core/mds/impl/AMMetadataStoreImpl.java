package com.latticeengines.apps.core.mds.impl;

import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(AMMetadataStoreImpl.class);

    private final ColumnMetadataProxy columnMetadataProxy;
    private final static Scheduler scheduler = Schedulers.newParallel("am-metadata");

    @Inject
    public AMMetadataStoreImpl(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<String> namespace) {
        String dcVersion = namespace.getCoord1();
        AtomicLong counter = new AtomicLong();
        return getRawFlux(dcVersion) //
                .map(ColumnMetadata::clone) // column metadata proxy cache should be read only
                .doOnSubscribe(s -> counter.set(0)) //
                .doOnNext(cm -> counter.getAndIncrement()) //
                .doOnComplete(() -> log.info("Served " + counter.get() + " AM records for version " + dcVersion));
    }

    @Override
    public ParallelFlux<ColumnMetadata> getMetadataInParallel(Namespace1<String> namespace) {
        String dcVersion = namespace.getCoord1();
        ThreadLocal<AtomicLong> counter = new ThreadLocal<>();
        return getRawFlux(dcVersion) //
                .parallel().runOn(scheduler) //
                .map(ColumnMetadata::clone) // column metadata proxy cache should be read only
                .doOnNext(cm -> {
                    if (counter.get() == null) {
                        counter.set(new AtomicLong(0));
                    }
                    counter.get().getAndIncrement();
                }) //
                .doOnComplete(() -> {
                    long count = 0;
                    if (counter.get() != null) {
                        count = counter.get().get();
                    }
                    log.info("Served " + count + " AM records for version " + dcVersion);
                });
    }

    private Flux<ColumnMetadata> getRawFlux(String dcVersion) {
        return Mono.fromCallable(() -> columnMetadataProxy.getAllColumns(dcVersion)).flatMapMany(Flux::fromIterable);
    }

}

package com.latticeengines.apps.core.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class AMMetadataStoreImpl implements AMMetadataStore {

    private final ColumnMetadataProxy columnMetadataProxy;

    @Inject
    public AMMetadataStoreImpl(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<String> namespace) {
        String dcVersion = namespace.getCoord1();
        return Mono.fromCallable(() -> columnMetadataProxy.getAllColumns(dcVersion)) //
                .flatMapMany(Flux::fromIterable);
    }

}

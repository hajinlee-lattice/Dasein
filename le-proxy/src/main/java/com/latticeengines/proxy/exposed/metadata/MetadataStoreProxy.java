package com.latticeengines.proxy.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;

public interface MetadataStoreProxy {

    Flux<ColumnMetadata> getMetadata(String mdsName, String... namespace);

}

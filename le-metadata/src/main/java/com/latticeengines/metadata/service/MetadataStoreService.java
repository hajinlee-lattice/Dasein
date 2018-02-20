package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;

public interface MetadataStoreService {

    Flux<ColumnMetadata> getMetadata(String metadataStoreName, String... namespace);

}

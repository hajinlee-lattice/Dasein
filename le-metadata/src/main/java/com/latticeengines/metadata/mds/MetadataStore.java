package com.latticeengines.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;

public interface MetadataStore {

    String getName();

    Flux<ColumnMetadata> getMetadata(String... namespace);

}

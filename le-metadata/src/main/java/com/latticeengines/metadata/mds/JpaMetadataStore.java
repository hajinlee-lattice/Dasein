package com.latticeengines.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;

public interface JpaMetadataStore {

    String getName();

    Flux<ColumnMetadata> getMetadata(Serializable... namespace);

    Serializable[] parseNameSpace(String... namespace);

}

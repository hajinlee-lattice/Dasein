package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Named;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;

public interface MetadataStore<N extends Namespace> extends Named {

    Flux<ColumnMetadata> getMetadata(N namespace);

}

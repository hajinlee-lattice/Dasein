package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Named;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public interface MetadataStore<N extends Namespace> extends Named {

    Scheduler scheduler = Schedulers.newParallel("metadata-store");

    Flux<ColumnMetadata> getMetadata(N namespace);

}

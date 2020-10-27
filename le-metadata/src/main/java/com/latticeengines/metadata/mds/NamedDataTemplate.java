package com.latticeengines.metadata.mds;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public interface NamedDataTemplate<N extends Namespace> extends DataUnitStore<N> {

    Scheduler scheduler = Schedulers.newParallel("data-template");

    String getName();

    N parseNameSpace(String... namespace);

    long countSchema(N namespace);

}

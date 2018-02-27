package com.latticeengines.proxy.exposed.metadata;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;

public interface MetadataStoreProxy {

    Flux<ColumnMetadata> getMetadata(String mdsName, String... namespace);

    <T extends Serializable> MetadataStore<Namespace1<T>> toMetadataStore(String mdsName, Class<T> clz);

    <T1 extends Serializable, T2 extends Serializable> MetadataStore<Namespace2<T1, T2>> toMetadataStore(String mdsName, Class<T1> clz1, Class<T2> clz2);

}

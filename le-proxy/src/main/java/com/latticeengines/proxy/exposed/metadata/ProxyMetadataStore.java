package com.latticeengines.proxy.exposed.metadata;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;

public class ProxyMetadataStore<N extends Namespace> implements MetadataStore<N> {

    private final MetadataStoreProxy metadataStoreProxy;
    private final String mdsName;

    public static <T extends Serializable> ProxyMetadataStore<Namespace1<T>> build(
            MetadataStoreProxy metadataStoreProxy, String mdsName, Class<T> clz) {
        return new ProxyMetadataStore<>(metadataStoreProxy, mdsName);
    }

    public static <T1 extends Serializable, T2 extends Serializable> ProxyMetadataStore<Namespace2<T1, T2>> build(
            MetadataStoreProxy metadataStoreProxy, String mdsName, Class<T1> clz1, Class<T2> clz2) {
        return new ProxyMetadataStore<>(metadataStoreProxy, mdsName);
    }

    private ProxyMetadataStore(MetadataStoreProxy metadataStoreProxy, String mdsName) {
        this.metadataStoreProxy = metadataStoreProxy;
        this.mdsName = mdsName;
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(N namespace) {
        Serializable[] coords = namespace.coords();
        String[] keys = new String[coords.length];
        for (int i = 0; i < coords.length; i++) {
            keys[i] = coords[i].toString();
        }
        return metadataStoreProxy.getMetadata(mdsName, keys);
    }

}

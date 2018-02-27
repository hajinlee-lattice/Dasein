package com.latticeengines.proxy.metadata;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataStoreProxy;
import com.latticeengines.proxy.exposed.metadata.ProxyMetadataStore;

import reactor.core.publisher.Flux;


@Component("metadataStoreProxy")
public class MetadataStoreProxyImpl extends MicroserviceRestApiProxy implements MetadataStoreProxy {

    protected MetadataStoreProxyImpl() {
        super("metadata/metadatastore");
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(String mdsName, String... namespace) {
        String url = constructUrl("/{mdsName}/namespace/{namespace}", mdsName,
                StringUtils.join(namespace, ","));
        return getFlux("get metadata", url, ColumnMetadata.class);
    }
    @Override
    public <T extends Serializable> MetadataStore<Namespace1<T>> toMetadataStore(String mdsName, Class<T> clz) {
        return ProxyMetadataStore.build(this, mdsName, clz);
    }

    @Override
    public <T1 extends Serializable, T2 extends Serializable> MetadataStore<Namespace2<T1, T2>> toMetadataStore(String mdsName, Class<T1> clz1, Class<T2> clz2) {
        return ProxyMetadataStore.build(this, mdsName, clz1, clz2);
    }


}

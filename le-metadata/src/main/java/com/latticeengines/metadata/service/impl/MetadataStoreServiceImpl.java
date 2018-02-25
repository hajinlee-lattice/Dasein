package com.latticeengines.metadata.service.impl;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.metadata.mds.JpaMetadataStore;
import com.latticeengines.metadata.mds.impl.AttrConfigMetadataStore;
import com.latticeengines.metadata.service.MetadataStoreService;

import reactor.core.publisher.Flux;

@Component("metadataStoreService")
public class MetadataStoreServiceImpl implements MetadataStoreService {

    @Inject
    private AttrConfigMetadataStore attrConfigMetadataStore;

    private ConcurrentMap<String, JpaMetadataStore> registry;

    public Flux<ColumnMetadata> getMetadata(String metadataStoreName, String... namespace) {
        JpaMetadataStore metadataStore = getMetadataStore(metadataStoreName);
        Serializable[] keys = metadataStore.parseNameSpace(namespace);
        return metadataStore.getMetadata(keys);
    }

    private JpaMetadataStore getMetadataStore(String metadataStoreName) {
        registerMetadataStores();
        if (registry.containsKey(metadataStoreName)) {
            return registry.get(metadataStoreName);
        } else {
            throw new RuntimeException("Cannot find metadata store named " + metadataStoreName);
        }
    }

    private void registerMetadataStores() {
        if (MapUtils.isEmpty(registry)) {
            registry = new ConcurrentHashMap<>();

            registerMetadataStore(attrConfigMetadataStore);
        }
    }

    private void registerMetadataStore(JpaMetadataStore metadataStore) {
        registry.put(metadataStore.getName(), metadataStore);
    }

}

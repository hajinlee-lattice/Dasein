package com.latticeengines.metadata.mds.impl;

import java.util.Arrays;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStoreName;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.mds.NamedMetadataStore;
import com.latticeengines.metadata.repository.AttrConfigRepository;
import com.latticeengines.metadata.repository.MetadataStoreRepository;

import reactor.core.publisher.Flux;

@Component("attrConfigMetadataStore")
public class AttrConfigMetadataStore extends JpaRepositoryMetadataStore<AttrConfigEntity>
        implements NamedMetadataStore<Namespace2<String, BusinessEntity>> {

    @Inject
    private AttrConfigRepository repository;

    @Override
    protected Class<AttrConfigEntity> getEntityClz() {
        return AttrConfigEntity.class;
    }

    @Override
    protected MetadataStoreRepository<AttrConfigEntity> getRepository() {
        return repository;
    }

    @Override
    public String getName() {
        return MetadataStoreName.AttrConfig;
    }

    @Override
    public Long count(Namespace2<String, BusinessEntity> namespace) {
        return count(namespace.coords());
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace2<String, BusinessEntity> namespace) {
        return getMetadata(namespace.coords());
    }

    @Override
    public Namespace2<String, BusinessEntity> parseNameSpace(String... namespace) {
        if (namespace.length != 2) {
            throw new IllegalArgumentException("The namespace for " + getName()
                    + " should have two coordinates, but found " + Arrays.toString(namespace));
        }
        String tenantId = namespace[0];
        BusinessEntity entity = BusinessEntity.valueOf(namespace[1]);
        return Namespace.as(tenantId, entity);
    }

}

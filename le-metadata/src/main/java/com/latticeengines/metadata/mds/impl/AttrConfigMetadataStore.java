package com.latticeengines.metadata.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.metadata.MetadataStoreName;
import com.latticeengines.metadata.mds.MetadataStore;
import com.latticeengines.metadata.repository.AttrConfigRepository;
import com.latticeengines.metadata.repository.MetadataStoreRepository;

@Component("attrConfigMetadataStore")
public class AttrConfigMetadataStore extends JpaRepositoryMetadataStore<AttrConfigEntity> implements MetadataStore {

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

}

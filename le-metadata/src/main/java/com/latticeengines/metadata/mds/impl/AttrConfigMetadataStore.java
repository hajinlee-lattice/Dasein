package com.latticeengines.metadata.mds.impl;

import java.io.Serializable;
import java.util.Arrays;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.metadata.MetadataStoreName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.mds.JpaMetadataStore;
import com.latticeengines.metadata.repository.AttrConfigRepository;
import com.latticeengines.metadata.repository.MetadataStoreRepository;

@Component("attrConfigMetadataStore")
public class AttrConfigMetadataStore extends JpaRepositoryMetadataStore<AttrConfigEntity> implements JpaMetadataStore {

    @Inject
    private AttrConfigRepository repository;

    @Override
    protected Class<AttrConfigEntity> getDAOClz() {
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
    public Serializable[] parseNameSpace(String... namespace) {
        if (namespace.length != 2) {
            throw new IllegalArgumentException("The namespace for " + getName()
                    + " should have two coordinates, but found " + Arrays.toString(namespace));
        }
        Serializable[] keys = new Serializable[namespace.length];
        keys[0] = namespace[0];
        keys[1] = BusinessEntity.valueOf(namespace[1]);
        return keys;
    }

}

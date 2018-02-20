package com.latticeengines.metadata.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.NoRepositoryBean;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;

@NoRepositoryBean
public interface MetadataStoreRepository<T> extends BaseJpaRepository<T, String> {

    long countByNameSpace(Class<T> clz, String... namespace);

    List<T> findByNamespace(Class<T> clz, Pageable pageable, String... namespace);

}

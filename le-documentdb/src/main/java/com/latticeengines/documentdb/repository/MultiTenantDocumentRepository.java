package com.latticeengines.documentdb.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;

public interface MultiTenantDocumentRepository<T> extends BaseJpaRepository<T, String> {

    @Transactional
    @Modifying
    List<T> removeByTenantId(String tenantId);

}

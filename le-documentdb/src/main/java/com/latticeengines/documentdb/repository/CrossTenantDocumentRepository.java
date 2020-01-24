package com.latticeengines.documentdb.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;

public interface CrossTenantDocumentRepository<T> extends BaseJpaRepository<T, String> {

}

package com.latticeengines.db.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.workflow.KeyValue;

public interface KeyValueRepository extends BaseJpaRepository<KeyValue, Long> {

    List<KeyValue> findByTenantId(long tenantId);

}

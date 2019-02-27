package com.latticeengines.metadata.repository.db;

import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;

@Transactional(readOnly = true)
public interface TableRepository extends BaseJpaRepository<Attribute, Long> {

    @Query("select t.pid from Table t where t.tenant.id = ?1 and t.name = ?2 and t.tableTypeCode = 0")
    Long findPidByTenantIdAndName(String tenantId, String tableName);

}

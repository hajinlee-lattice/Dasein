package com.latticeengines.db.exposed.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.KeyValue;


public interface KeyValueEntityMgr extends BaseEntityMgrRepository<KeyValue, Long> {

    List<KeyValue> findByTenantId(long tenantId);

}

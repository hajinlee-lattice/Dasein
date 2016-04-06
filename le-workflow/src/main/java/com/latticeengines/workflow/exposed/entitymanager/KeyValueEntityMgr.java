package com.latticeengines.workflow.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.workflow.KeyValue;


public interface KeyValueEntityMgr extends BaseEntityMgr<KeyValue> {

    List<KeyValue> findByTenantId(long tenantId);

}

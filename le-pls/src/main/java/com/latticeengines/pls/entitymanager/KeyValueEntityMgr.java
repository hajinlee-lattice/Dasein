package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.KeyValue;


public interface KeyValueEntityMgr extends BaseEntityMgr<KeyValue> {

    List<KeyValue> findByTenantId(long tenantId);

}

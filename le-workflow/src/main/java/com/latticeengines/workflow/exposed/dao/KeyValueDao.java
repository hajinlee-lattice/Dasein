package com.latticeengines.workflow.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.workflow.KeyValue;

public interface KeyValueDao extends BaseDao<KeyValue> {

    List<KeyValue> findByTenantId(long tenantId);
}

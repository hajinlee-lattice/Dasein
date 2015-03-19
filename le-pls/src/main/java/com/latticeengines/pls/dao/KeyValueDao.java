package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.KeyValue;

public interface KeyValueDao extends BaseDao<KeyValue> {

    List<KeyValue> findByTenantId(long tenantId);
}

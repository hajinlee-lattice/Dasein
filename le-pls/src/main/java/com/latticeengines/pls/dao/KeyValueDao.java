package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.KeyValue;

public interface KeyValueDao extends BaseDao<KeyValue> {

    KeyValue findByEntityId(Long entityPid);

    
}

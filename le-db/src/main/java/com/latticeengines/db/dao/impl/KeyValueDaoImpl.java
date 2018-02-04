package com.latticeengines.db.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.KeyValueDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.workflow.KeyValue;

@Component("keyValueDao")
public class KeyValueDaoImpl extends BaseDaoImpl<KeyValue> implements KeyValueDao {

    @Override
    protected Class<KeyValue> getEntityClass() {
        return KeyValue.class;
    }

}

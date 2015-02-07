package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.pls.dao.KeyValueDao;

@Component("keyValueDao")
public class KeyValueDaoImpl extends BaseDaoImpl<KeyValue> implements KeyValueDao {

    @Override
    protected Class<KeyValue> getEntityClass() {
        return KeyValue.class;
    }

}

package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.pls.dao.KeyValueDao;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;

@Component("keyValueEntityMgr")
public class KeyValueEntityMgrImpl extends BaseEntityMgrImpl<KeyValue> implements KeyValueEntityMgr {

    @Autowired
    private KeyValueDao keyValueDao;
    
    @Override
    public BaseDao<KeyValue> getDao() {
        return keyValueDao;
    }

}

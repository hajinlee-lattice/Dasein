package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<KeyValue> findByTenantId(long tenantId) {
        return keyValueDao.findByTenantId(tenantId);
    }
}

package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.pls.dao.KeyValueDao;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.BasePLSEntityMgrImpl;

@Component("keyValueEntityMgr")
public class KeyValueEntityMgrImpl extends BasePLSEntityMgrImpl<KeyValue> implements KeyValueEntityMgr {

    @Autowired
    private KeyValueDao keyValueDao;
    
    @Override
    public BaseDao<KeyValue> getDao() {
        return keyValueDao;
    }

    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<KeyValue> findByTenantId(long tenantId) {
        return keyValueDao.findByTenantId(tenantId);
    }
}

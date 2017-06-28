package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.pls.dao.DLTenantMappingDao;
import com.latticeengines.pls.entitymanager.DLTenantMappingEntityMgr;

@Component("dlTenantMappingEntityMgr")
public class DLTenantMappingEntityMgrImpl extends BaseEntityMgrImpl<DLTenantMapping> implements DLTenantMappingEntityMgr {

    @Autowired
    private DLTenantMappingDao dlTenantMappingDao;

    @Override
    public BaseDao<DLTenantMapping> getDao() {
        return dlTenantMappingDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup) {
        return dlTenantMappingDao.getDLTenantMapping(dlTenantId, dlLoadGroup);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DLTenantMapping getDLTenantMapping(String dlTenantId) {
        return findByField("DL_TENANT_ID", dlTenantId);
    }
}

package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.CDLExternalSystemDao;
import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;

@Component("cdlExternalSystemEntityMgr")
public class CDLExternalSystemEntityMgrImpl extends BaseEntityMgrImpl<CDLExternalSystem> implements CDLExternalSystemEntityMgr {

    @Autowired
    private CDLExternalSystemDao cdlExternalSystemDao;

    @Override
    public BaseDao<CDLExternalSystem> getDao() {
        return cdlExternalSystemDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<CDLExternalSystem> findAllExternalSystem() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CDLExternalSystem findExternalSystem() {
        List<CDLExternalSystem> allExternalSystem = super.findAll();
        if (allExternalSystem == null || allExternalSystem.size() == 0) {
            return null;
        } else {
            return allExternalSystem.get(0);
        }
    }
}

package com.latticeengines.apps.cdl.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.CDLJobDetailDao;
import com.latticeengines.apps.cdl.entitymanager.CDLJobDetailEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;

@Component("cdlJobDetailEntityMgr")
public class CDLJobDetailEntityMgrImpl extends BaseEntityMgrImpl<CDLJobDetail> implements CDLJobDetailEntityMgr {

    @Autowired
    private CDLJobDetailDao cdlJobDetailDao;

    @Override
    public BaseDao<CDLJobDetail> getDao() {
        return cdlJobDetailDao;
    }
}

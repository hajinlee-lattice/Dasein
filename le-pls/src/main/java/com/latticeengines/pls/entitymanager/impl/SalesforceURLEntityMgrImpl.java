package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.dao.SalesforceURLDao;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;

@Component("salesforceURLEntityMgr")
public class SalesforceURLEntityMgrImpl extends BaseEntityMgrImpl<SalesforceURL> implements SalesforceURLEntityMgr {

    @Autowired
    private SalesforceURLDao salesforceURLDao;

    @Override
    public BaseDao<SalesforceURL> getDao() {
        return salesforceURLDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SalesforceURL findByURLName(String urlName) {
        return salesforceURLDao.findByURLName(urlName);
    }
}
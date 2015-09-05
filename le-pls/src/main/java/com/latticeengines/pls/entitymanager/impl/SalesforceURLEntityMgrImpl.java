package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.dao.SalesforceURLDao;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.BasePLSEntityMgrImpl;

@Component("salesforceURLEntityMgr")
public class SalesforceURLEntityMgrImpl extends BasePLSEntityMgrImpl<SalesforceURL> implements SalesforceURLEntityMgr {

    @Autowired
    private SalesforceURLDao salesforceURLDao;

    @Override
    public BaseDao<SalesforceURL> getDao() {
        return salesforceURLDao;
    }

    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SalesforceURL findByURLName(String urlName) {
        return salesforceURLDao.findByURLName(urlName);
    }
}
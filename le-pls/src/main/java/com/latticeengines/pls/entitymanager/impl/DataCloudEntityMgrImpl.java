package com.latticeengines.pls.entitymanager.impl;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.pls.dao.DataCloudDao;
import com.latticeengines.pls.entitymanager.DataCloudEntityMgr;
@Component("dataCloudEntityMgr")
public class DataCloudEntityMgrImpl extends BaseEntityMgrImpl<CustomerReport> implements DataCloudEntityMgr {

    @Autowired
    private DataCloudDao dataCloudDao;
    @Override
    public BaseDao<CustomerReport> getDao() {
        return dataCloudDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CustomerReport findById(String id) {
        return dataCloudDao.findById(id);
    }

}

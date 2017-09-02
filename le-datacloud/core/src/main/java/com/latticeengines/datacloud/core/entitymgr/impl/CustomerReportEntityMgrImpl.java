package com.latticeengines.datacloud.core.entitymgr.impl;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.CustomerReportDao;
import com.latticeengines.datacloud.core.entitymgr.CustomerReportEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
@Component("customerReportEntityMgr")
public class CustomerReportEntityMgrImpl extends BaseEntityMgrImpl<CustomerReport> implements CustomerReportEntityMgr {

    @Autowired
    private CustomerReportDao customerReportDao;
    @Override
    public BaseDao<CustomerReport> getDao() {
        return customerReportDao;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CustomerReport findById(String id) {
        return customerReportDao.findById(id);
    }

}

package com.latticeengines.datacloud.core.entitymgr.impl;


import java.util.List;

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

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @Override
    public void create(CustomerReport entity) {
        getDao().create(entity);
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(CustomerReport entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @Override
    public void update(CustomerReport entity) {
        getDao().update(entity);
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @Override
    public void delete(CustomerReport entity) {
        getDao().delete(entity);
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public boolean containInSession(CustomerReport entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public CustomerReport findByKey(CustomerReport entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CustomerReport findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public List<CustomerReport> findAll() {
        return getDao().findAll();
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CustomerReport findById(String id) {
        return customerReportDao.findById(id);
    }

}

package com.latticeengines.apps.cdl.entitymgr.impl;


import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.BusinessCalendarDao;
import com.latticeengines.apps.cdl.entitymgr.BusinessCalendarEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.BusinessCalendarRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

@Component("businessCalendarEntityMgr")
public class BusinessCalendarEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BusinessCalendar, Long> implements BusinessCalendarEntityMgr {


    private static final Logger log = LoggerFactory.getLogger(BusinessCalendarEntityMgrImpl.class);

    @Inject
    private BusinessCalendarRepository repository;

    @Inject
    private BusinessCalendarDao dao;

    @Override
    public BaseJpaRepository<BusinessCalendar, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<BusinessCalendar> getDao() {
        return dao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public BusinessCalendar find() {
        return repository.findAll().stream().findFirst().orElse(null);
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public BusinessCalendar save(BusinessCalendar businessCalendar) {
        Tenant tenant = MultiTenantContext.getTenant();
        BusinessCalendar existing = repository.findByTenant(tenant);
        if (existing != null) {
            businessCalendar.setPid(existing.getPid());
        }
        businessCalendar.setTenant(tenant);
        super.createOrUpdate(businessCalendar);
        return businessCalendar;
    }

}

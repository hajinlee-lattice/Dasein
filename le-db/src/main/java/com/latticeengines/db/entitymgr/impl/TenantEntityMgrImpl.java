package com.latticeengines.db.entitymgr.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.TenantDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.repository.TenantRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;

@Component("tenantEntityMgr")
public class TenantEntityMgrImpl extends BaseEntityMgrRepositoryImpl<Tenant, Long> implements TenantEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(TenantEntityMgrImpl.class);

    private final TenantRepository tenantRepository;

    private final TenantDao tenantDao;

    @Inject
    public TenantEntityMgrImpl(TenantRepository tenantRepository, TenantDao tenantDao) {
        this.tenantRepository = tenantRepository;
        this.tenantDao = tenantDao;
    }

    @Override
    public BaseJpaRepository<Tenant, Long> getRepository() {
        return tenantRepository;
    }

    @Override
    public BaseDao<Tenant> getDao() {
        return tenantDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantPid(Long tenantPid) {
        return tenantRepository.findById(tenantPid).orElse(null);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantId(String tenantId) {
        return tenantRepository.findByTenantId(tenantId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getAllTenantId() {
        return tenantRepository.findAllTenantId();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Tenant findByTenantName(String tenantName) {
        return tenantRepository.findByName(tenantName);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Tenant> findAllByStatus(TenantStatus status) {
        return tenantRepository.findAllByStatus(status);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Tenant> findAllByType(TenantType type) {
        return tenantRepository.findAllByTenantType(type);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void setNotificationStateByTenantId(String tenantId, int status) {
        Tenant tenant1 = findByTenantId(tenantId);
        if (tenant1 == null) {
            log.error("can not find the tenant using tenantId " + tenantId);
        } else {
            tenant1.setNotificationLevel(TenantEmailNotificationLevel.getNameByNum(status));
            super.update(tenant1);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(Tenant tenant) {
        if (tenant.getRegisteredTime() == null) {
            tenant.setRegisteredTime(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        // only POC tenant has expire time
        if (TenantType.POC.equals(tenant.getTenantType()) && tenant.getExpiredTime() == null) {
            // expired date = registered + 90
            Long expiredTime = tenant.getRegisteredTime() + TimeUnit.DAYS.toMillis(90);
            tenant.setExpiredTime(expiredTime);
        }
        super.create(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void delete(Tenant tenant) {
        Tenant tenant1 = findByTenantId(tenant.getId());
        super.delete(tenant1);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void update(Tenant tenant) {
        super.update(tenant);
    }


}

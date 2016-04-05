package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.QuotaDao;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("quotaEntityMgr")
public class QuotaEntityMgrImpl extends BaseEntityMgrImpl<Quota> implements
        QuotaEntityMgr {

    @Autowired
    QuotaDao quotaDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<Quota> getDao() {
        return this.quotaDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Quota quota) {
        if (this.quotaDao.findQuotaByQuotaId(quota.getId()) != null) {
            throw new RuntimeException(String.format(
                    "Quota for tenant: %s already exists", MultiTenantContext
                            .getTenant().getName()));
        }
        this.setTenantAndTenantIdOnQuota(quota);
        this.quotaDao.create(quota);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteQuotaByQuotaId(String quotaId) {
        Quota quota = this.quotaDao.findQuotaByQuotaId(quotaId);
        this.quotaDao.delete(quota);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Quota findQuotaByQuotaId(String quotaId) {
        return this.quotaDao.findQuotaByQuotaId(quotaId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Quota> getAllQuotas() {
        return this.quotaDao.findAll();
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateQuotaByQuotaId(Quota quota, String quotaId) {
        if (this.findQuotaByQuotaId(quotaId) != null) {
            this.deleteQuotaByQuotaId(quotaId);
        }
        
        this.create(quota);
    }

    private void setTenantAndTenantIdOnQuota(Quota quota) {
        Tenant tenant = this.tenantEntityMgr
                .findByTenantId(MultiTenantContext.getTenant().getId());
        quota.setTenant(tenant);
        quota.setTenantId(tenant.getPid());
    }

}

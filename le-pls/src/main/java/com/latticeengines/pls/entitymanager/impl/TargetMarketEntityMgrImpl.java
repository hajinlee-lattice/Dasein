package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TargetMarketDao;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("targetMarketEntityMgr")
public class TargetMarketEntityMgrImpl extends BaseEntityMgrImpl<TargetMarket>
        implements TargetMarketEntityMgr {

    @Autowired
    TargetMarketDao targetMarketDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<TargetMarket> getDao() {
        return this.targetMarketDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TargetMarket targetMarket) {
        TargetMarket targetMarketStored = this.targetMarketDao
                .findTargetMarketByName(targetMarket.getName());
        if (targetMarketStored != null) {
            throw new RuntimeException(String.format(
                    "Target market with name %s already exists",
                    targetMarket.getName()));
        }
        this.setTenantAndTenantIdOnTargetMarket(targetMarket);
        this.targetMarketDao.create(targetMarket);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteTargetMarketByName(String name) {
        TargetMarket targetMarket = this.targetMarketDao
                .findTargetMarketByName(name);
        this.targetMarketDao.delete(targetMarket);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TargetMarket findTargetMarketByName(String name) {
        return this.targetMarketDao.findTargetMarketByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TargetMarket> getAllTargetMarkets() {
        return this.targetMarketDao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        if (this.findTargetMarketByName(name) != null) {
            this.deleteTargetMarketByName(name);
        }
        
        this.create(targetMarket);
    }

    private void setTenantAndTenantIdOnTargetMarket(TargetMarket targetMarket) {
        Tenant tenant = this.tenantEntityMgr
                .findByTenantId(SecurityContextUtils.getTenant().getId());
        targetMarket.setTenant(tenant);
        targetMarket.setTenantId(tenant.getPid());
    }

}

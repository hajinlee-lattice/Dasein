package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOption;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TargetMarketDao;
import com.latticeengines.pls.dao.TargetMarketDataFlowOptionDao;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("targetMarketEntityMgr")
public class TargetMarketEntityMgrImpl extends BaseEntityMgrImpl<TargetMarket> implements TargetMarketEntityMgr {

    @Autowired
    TargetMarketDao targetMarketDao;

    @Autowired
    TargetMarketDataFlowOptionDao targetMarketDataflowOptionDao;
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<TargetMarket> getDao() {
        return targetMarketDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TargetMarket targetMarket) {
        TargetMarket targetMarketStored = targetMarketDao.findTargetMarketByName(targetMarket.getName());
        if (targetMarketStored != null) {
            throw new RuntimeException(String.format("Target market with name %s already exists",
                    targetMarket.getName()));
        }
        initializeForDatabaseEntry(targetMarket);
        targetMarketDao.create(targetMarket);
        for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
            targetMarketDataflowOptionDao.create(option);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteTargetMarketByName(String name) {
        TargetMarket targetMarket = targetMarketDao.findTargetMarketByName(name);
        targetMarketDao.delete(targetMarket);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TargetMarket findTargetMarketByName(String name) {
        TargetMarket market = targetMarketDao.findTargetMarketByName(name);
        return market;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TargetMarket> getAllTargetMarkets() {
        return targetMarketDao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        if (findTargetMarketByName(name) != null) {
            deleteTargetMarketByName(name);
        }

        create(targetMarket);
    }

    private void initializeForDatabaseEntry(TargetMarket targetMarket) {
        Tenant tenant = tenantEntityMgr.findByTenantId(SecurityContextUtils.getTenant().getId());
        targetMarket.setTenant(tenant);
        targetMarket.setTenantId(tenant.getPid());
        targetMarket.setPid(null);

        for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
            option.setPid(null);
            option.setTargetMarket(targetMarket);
        }
    }

}

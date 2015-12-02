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
import com.latticeengines.domain.exposed.pls.TargetMarketStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TargetMarketDao;
import com.latticeengines.pls.dao.TargetMarketDataFlowOptionDao;
import com.latticeengines.pls.dao.TargetMarketStatisticsDao;
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
    TargetMarketStatisticsDao targetMarketStatisticsDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<TargetMarket> getDao() {
        return this.targetMarketDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TargetMarket targetMarket) {
        TargetMarket targetMarketStored = this.targetMarketDao.findTargetMarketByName(targetMarket.getName());
        if (targetMarketStored != null) {
            throw new RuntimeException(String.format("Target market with name %s already exists",
                    targetMarket.getName()));
        }
        if (targetMarket.getIsDefault() && targetMarketDao.findDefaultTargetMarket() != null) {
            throw new RuntimeException("Only one default market can be created");
        }
        initializeForDatabaseEntry(targetMarket);

        TargetMarketStatistics targetMarketStatistics = targetMarket.getTargetMarketStatistics();
        targetMarketStatistics.setPid(null);
        this.targetMarketStatisticsDao.create(targetMarketStatistics);

        this.targetMarketDao.create(targetMarket);
        for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
            this.targetMarketDataflowOptionDao.create(option);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteTargetMarketByName(String name) {
        TargetMarket targetMarket = targetMarketDao.findTargetMarketByName(name);
        this.targetMarketDao.delete(targetMarket);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TargetMarket findTargetMarketByName(String name) {
        TargetMarket market = this.targetMarketDao.findTargetMarketByName(name);
        return market;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TargetMarket> getAllTargetMarkets() {
        return this.targetMarketDao.findAll();
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
        Tenant tenant = this.tenantEntityMgr.findByTenantId(SecurityContextUtils.getTenant().getId());
        targetMarket.setTenant(tenant);
        targetMarket.setTenantId(tenant.getPid());
        targetMarket.setPid(null);

        for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
            option.setPid(null);
            option.setTargetMarket(targetMarket);
        }

    }

}

package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.hibernate.Hibernate;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOption;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.domain.exposed.pls.TargetMarketStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TargetMarketDao;
import com.latticeengines.pls.dao.TargetMarketDataFlowOptionDao;
import com.latticeengines.pls.dao.TargetMarketReportMapDao;
import com.latticeengines.pls.dao.TargetMarketStatisticsDao;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("targetMarketEntityMgr")
public class TargetMarketEntityMgrImpl extends BaseEntityMgrImpl<TargetMarket> implements TargetMarketEntityMgr {

    @Autowired
    private TargetMarketDao targetMarketDao;

    @Autowired
    private TargetMarketDataFlowOptionDao targetMarketDataflowOptionDao;

    @Autowired
    private TargetMarketStatisticsDao targetMarketStatisticsDao;

    @Autowired
    private TargetMarketReportMapDao targetMarketReportMapDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<TargetMarket> getDao() {
        return targetMarketDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TargetMarket targetMarket) {
        targetMarket.setCreationTimestampObject(DateTime.now());
        initializeForDatabaseEntry(targetMarket);

        TargetMarketStatistics targetMarketStatistics = targetMarket.getTargetMarketStatistics();
        if (targetMarketStatistics != null) {
            targetMarketStatistics.setPid(null);
            targetMarketStatisticsDao.create(targetMarketStatistics);
        }

        targetMarketDao.create(targetMarket);
        if (targetMarket.getDataFlowConfiguration() != null) {
            for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
                targetMarketDataflowOptionDao.create(option);
            }
        }

        if (targetMarket.getReports() != null) {
            for (TargetMarketReportMap reportMap : targetMarket.getReports()) {
                targetMarketReportMapDao.create(reportMap);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public TargetMarket createDefaultTargetMarket() {
        TargetMarket targetMarket = new TargetMarket();
        targetMarket.setName(TargetMarket.DEFAULT_NAME);
        targetMarket.setDescription("Default Market");
        targetMarket.setOffset(0);
        targetMarket.setIsDefault(true);
        targetMarket.setEventColumnName("");
        create(targetMarket);
        return targetMarket;
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
        inflate(market);
        return market;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TargetMarket> getAllTargetMarkets() {
        List<TargetMarket> markets = targetMarketDao.findAll();
        for (TargetMarket market : markets) {
            inflate(market);
        }
        return markets;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        if (findTargetMarketByName(name) != null) {
            deleteTargetMarketByName(name);
        }

        create(targetMarket);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TargetMarket> findAll() {
        return super.findAll();
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

    private void inflate(TargetMarket market) {
        if (market != null) {
            Hibernate.initialize(market.getReports());
            Hibernate.initialize(market.getDataFlowConfiguration());
        }
    }
}

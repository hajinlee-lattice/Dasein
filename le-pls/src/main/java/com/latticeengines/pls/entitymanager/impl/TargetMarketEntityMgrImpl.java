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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.TargetMarketDao;
import com.latticeengines.pls.dao.TargetMarketDataFlowOptionDao;
import com.latticeengines.pls.dao.TargetMarketReportMapDao;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.entitymanager.ReportEntityMgr;

@Component("targetMarketEntityMgr")
public class TargetMarketEntityMgrImpl extends BaseEntityMgrImpl<TargetMarket> implements TargetMarketEntityMgr {

    @Autowired
    private TargetMarketDao targetMarketDao;

    @Autowired
    private TargetMarketDataFlowOptionDao targetMarketDataflowOptionDao;

    @Autowired
    private TargetMarketReportMapDao targetMarketReportMapDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Override
    public BaseDao<TargetMarket> getDao() {
        return targetMarketDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TargetMarket targetMarket) {
        targetMarket.setCreationTimestampObject(DateTime.now());
        initializeForDatabaseEntry(targetMarket);

        targetMarketDao.create(targetMarket);
        if (targetMarket.getDataFlowConfiguration() != null) {
            for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
                targetMarketDataflowOptionDao.create(option);
            }
        }

        if (targetMarket.getReports() != null) {
            for (TargetMarketReportMap reportMap : targetMarket.getReports()) {
                reportEntityMgr.createOrUpdate(reportMap.getReport());
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
        if (targetMarket != null) {
            targetMarketDao.delete(targetMarket);
        }
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
    public List<TargetMarket> findAllTargetMarkets() {
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
        List<TargetMarket> all = super.findAll();
        for (TargetMarket market : all) {
            inflate(market);
        }
        return all;
    }

    private void initializeForDatabaseEntry(TargetMarket targetMarket) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        targetMarket.setTenant(tenant);
        targetMarket.setTenantId(tenant.getPid());
        targetMarket.setPid(null);

        if (targetMarket.getRawDataFlowConfiguration() != null) {
            for (TargetMarketDataFlowOption option : targetMarket.getRawDataFlowConfiguration()) {
                option.setPid(null);
                option.setTargetMarket(targetMarket);
            }
        }

        if (targetMarket.getReports() != null) {
            for (TargetMarketReportMap map : targetMarket.getReports()) {
                map.setPid(null);
                map.setTargetMarket(targetMarket);
                if (map.getReport() != null) {
                    map.getReport().setPid(null);
                    if (map.getReport().getJson() != null) {
                        map.getReport().getJson().setPid(null);
                    }
                }
            }
        }
    }

    private void inflate(TargetMarket market) {
        if (market != null) {
            Hibernate.initialize(market.getReports());
            Hibernate.initialize(market.getDataFlowConfiguration());
            for (TargetMarketReportMap map : market.getReports()) {
                Hibernate.initialize(map.getReport());
                Hibernate.initialize(map.getReport().getJson());
            }
        }
    }
}

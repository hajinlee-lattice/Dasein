package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PurchaseMetricsDao;
import com.latticeengines.apps.cdl.entitymgr.PurchaseMetricsEntityMgr;
import com.latticeengines.apps.cdl.repository.PurchaseMetricsRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

@Component("purchaseMetricsEntityMgrImpl")
public class PurchaseMetricsEntityMgrImpl extends BaseEntityMgrRepositoryImpl<PurchaseMetrics, Long>
        implements PurchaseMetricsEntityMgr {
    @Inject
    private PurchaseMetricsRepository repository;

    @Inject
    private PurchaseMetricsDao dao;

    @Override
    public BaseJpaRepository<PurchaseMetrics, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<PurchaseMetrics> getDao() {
        return dao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<PurchaseMetrics> findAll() { // filter by TenantID
        return repository.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<PurchaseMetrics> findAllActive() { // filter by TenantID
        return repository.findAllByIsEOL(false);
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public List<PurchaseMetrics> save(List<PurchaseMetrics> metricsList) {
        if (metricsList == null) {
            metricsList = new ArrayList<>();
        }

        Tenant tenant = MultiTenantContext.getTenant();
        metricsList.forEach(metrics -> {
            metrics.setTenant(tenant);
            metrics.setEOL(false);
            metrics.setDeprecated(null);
        });

        List<PurchaseMetrics> existingList = repository.findAllByTenant(tenant);
        if (CollectionUtils.isEmpty(existingList)) {
            metricsList.forEach(metrics -> {
                super.createOrUpdate(metrics);
            });
            return metricsList;
        }

        Map<InterfaceName, PurchaseMetrics> existingMetrics = new HashMap<>();
        existingList.forEach(existing -> {
            existingMetrics.put(existing.getMetrics(), existing);
        });
        Set<InterfaceName> selectedMetrics = new HashSet<>();
        metricsList.forEach(metrics -> {
            selectedMetrics.add(metrics.getMetrics());
        });
        for (PurchaseMetrics existing : existingList) {
            if (!selectedMetrics.contains(existing.getMetrics())) {
                existing.setEOL(true);
                existing.setDeprecated(new Date());
                metricsList.add(existing);
            }
        }
        metricsList.forEach(metrics -> {
            if (existingMetrics.containsKey(metrics.getMetrics())) {
                metrics.setPid(existingMetrics.get(metrics.getMetrics()).getPid());
                metrics.setCreated(existingMetrics.get(metrics.getMetrics()).getCreated());
            }
            super.createOrUpdate(metrics);
        });
        return metricsList;
    }

}

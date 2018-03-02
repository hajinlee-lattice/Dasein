package com.latticeengines.apps.cdl.entitymgr.impl;

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
    public List<PurchaseMetrics> findAll() { // with filter on TenantID
        return repository.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public void deleteAll() { // with filter on TenantID
        repository.deleteAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public List<PurchaseMetrics> save(List<PurchaseMetrics> metricsList) {
        if (CollectionUtils.isEmpty(metricsList)) {
            deleteAll();
            return metricsList;
        }

        Tenant tenant = MultiTenantContext.getTenant();
        metricsList.forEach(metrics -> {
            metrics.setTenant(tenant);
        });

        List<PurchaseMetrics> existingList = findAll();
        if (CollectionUtils.isEmpty(existingList)) {
            metricsList.forEach(metrics -> {
                super.createOrUpdate(metrics);
            });
            return metricsList;
        }

        // To maintain Ids for the purpose of auditing fields
        Map<InterfaceName, Long> existingIds = new HashMap<>();
        existingList.forEach(existing -> {
            existingIds.put(existing.getMetrics(), existing.getPid());
        });
        Set<InterfaceName> selectedMetrics = new HashSet<>();
        metricsList.forEach(metrics -> {
            selectedMetrics.add(metrics.getMetrics());
        });
        existingList.forEach(existing -> {
            if (!selectedMetrics.contains(existing.getMetrics())) {
                repository.delete(existing);
            }
        });
        metricsList.forEach(metrics -> {
            if (existingIds.containsKey(metrics.getMetrics())) {
                metrics.setPid(existingIds.get(metrics.getMetrics()));
            }
            super.createOrUpdate(metrics);
        });
        return metricsList;
    }

}

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

import com.latticeengines.apps.cdl.dao.ActivityMetricsDao;
import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.repository.ActivityMetricsRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

@Component("activityMetricsEntityMgrImpl")
public class ActivityMetricsEntityMgrImpl extends BaseEntityMgrRepositoryImpl<ActivityMetrics, Long>
        implements ActivityMetricsEntityMgr {
    @Inject
    private ActivityMetricsRepository repository;

    @Inject
    private ActivityMetricsDao dao;

    @Override
    public BaseJpaRepository<ActivityMetrics, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<ActivityMetrics> getDao() {
        return dao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<ActivityMetrics> findAll() { // filter by TenantID
        return repository.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<ActivityMetrics> findAllActive() { // filter by TenantID
        return repository.findAllByIsEOL(false);
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public List<ActivityMetrics> save(List<ActivityMetrics> metricsList) {
        if (metricsList == null) {
            metricsList = new ArrayList<>();
        }

        Tenant tenant = MultiTenantContext.getTenant();
        metricsList.forEach(metrics -> {
            metrics.setTenant(tenant);
            metrics.setEOL(false);
            metrics.setDeprecated(null);
        });

        List<ActivityMetrics> existingList = repository.findAllByTenant(tenant);
        if (CollectionUtils.isEmpty(existingList)) {
            metricsList.forEach(metrics -> {
                super.createOrUpdate(metrics);
            });
            return metricsList;
        }

        Map<InterfaceName, ActivityMetrics> existingMetrics = new HashMap<>();
        existingList.forEach(existing -> {
            existingMetrics.put(existing.getMetrics(), existing);
        });
        Set<InterfaceName> selectedMetrics = new HashSet<>();
        metricsList.forEach(metrics -> {
            selectedMetrics.add(metrics.getMetrics());
        });
        for (ActivityMetrics existing : existingList) {
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

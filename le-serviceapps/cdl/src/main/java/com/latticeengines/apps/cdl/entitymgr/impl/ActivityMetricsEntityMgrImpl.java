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
import com.latticeengines.apps.cdl.repository.writer.ActivityMetricsRepository;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsActionConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

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
    public List<ActivityMetrics> findWithType(ActivityType type) { // filter by TenantID
        List<ActivityMetrics> metrics = repository.findAllByType(type);
        metrics.forEach(m -> {
            m.getPeriodsConfig();
        });
        return metrics;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<ActivityMetrics> findActiveWithType(ActivityType type) { // filter by TenantID
        List<ActivityMetrics> metrics = repository.findAllByIsEOLAndType(false, type);
        metrics.forEach(m -> {
            m.getPeriodsConfig();
        });
        return metrics;
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public List<ActivityMetrics> save(List<ActivityMetrics> metricsList) {
        if (metricsList == null) {
            metricsList = new ArrayList<>();
        }

        ActivityMetricsUtils.isValidMetrics(metricsList);

        Tenant tenant = MultiTenantContext.getTenant();
        metricsList.forEach(metrics -> {
            metrics.setTenant(tenant);
            metrics.setEOL(false);
            metrics.setDeprecated(null);
            metrics.setPeriods();
        });

        // for action info: metrics displayname without period
        Set<String> activated = new HashSet<>();
        Set<String> updated = new HashSet<>();
        Set<String> deactivated = new HashSet<>();

        List<ActivityMetrics> existingList = repository.findAllByTenant(tenant);
        if (CollectionUtils.isEmpty(existingList)) {
            metricsList.forEach(metrics -> {
                super.createOrUpdate(metrics);
                activated.add(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()));
            });
            createAction(MultiTenantContext.getEmailAddress(), new ArrayList<>(activated), new ArrayList<>(updated),
                    new ArrayList<>(deactivated));
            return metricsList;
        }


        Map<String, ActivityMetrics> existingMetrics = new HashMap<>();
        existingList.forEach(existing -> {
            existingMetrics.put(ActivityMetricsUtils.getNameWithPeriod(existing), existing);
        });
        Set<String> selectedMetrics = new HashSet<>();
        metricsList.forEach(metrics -> {
            selectedMetrics.add(ActivityMetricsUtils.getNameWithPeriod(metrics));
        });
        for (ActivityMetrics existing : existingList) {
            if (!selectedMetrics.contains(ActivityMetricsUtils.getNameWithPeriod(existing))) {
                existing.setEOL(true);
                existing.setDeprecated(new Date());
                super.createOrUpdate(existing);
            }
            // Put all existing ones to deactivated first, might move to updated later
            deactivated.add(ActivityMetricsUtils.getMetricsDisplayName(existing.getMetrics()));
        }
        for (ActivityMetrics metrics : metricsList) {
            if (existingMetrics.containsKey(ActivityMetricsUtils.getNameWithPeriod(metrics))) {
                metrics.setPid(existingMetrics.get(ActivityMetricsUtils.getNameWithPeriod(metrics)).getPid());
                metrics.setCreated(existingMetrics.get(ActivityMetricsUtils.getNameWithPeriod(metrics)).getCreated());
            }
            if (deactivated.contains(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()))) {
                deactivated.remove(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()));
                updated.add(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()));
            } else if (!updated.contains(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()))) {
                activated.add(ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics()));
            }
            super.createOrUpdate(metrics);
        }

        createAction(MultiTenantContext.getEmailAddress(), new ArrayList<>(activated), new ArrayList<>(updated),
                new ArrayList<>(deactivated));
        return metricsList;
    }

    private void createAction(String initiator, List<String> activated, List<String> updated,
            List<String> deactivated) {
        Action action = new Action();
        action.setType(ActionType.ACTIVITY_METRICS_CHANGE);
        action.setActionInitiator(initiator);
        ActivityMetricsActionConfiguration config = new ActivityMetricsActionConfiguration(activated, updated,
                deactivated);
        action.setActionConfiguration(config);
        ActionContext.setAction(action);
    }

}

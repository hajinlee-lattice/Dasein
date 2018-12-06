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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsActionConfiguration;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
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

    /*
     * For PA, need to get all the metrics including deprecated
     */
    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<ActivityMetrics> findWithType(ActivityType type) { // filter by TenantID
        List<ActivityMetrics> metrics = repository.findAllByType(type);
        metrics.forEach(m -> {
            m.getPeriodsConfig();
        });
        return metrics;
    }

    /*
     * For metrics configuration UI:
     * 
     * 1. Need to exclude deprecated metrics
     * 
     * 2. TotalSpendOvertime & AvgSpendOvertime changed comparison type in time
     * filter from WITHIN to BETWEEN in M26. If PA, we still support both
     * calculation since we need ensure backward compatibility, but for metrics
     * configuration UI, old WITHIN will be translated to BETWEEN
     */
    @Override
    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<ActivityMetrics> findActiveWithType(ActivityType type) { // filter by TenantID
        List<ActivityMetrics> metrics = repository.findAllByIsEOLAndType(false, type);
        metrics.forEach(m -> {
            List<TimeFilter> timeFilters = m.getPeriodsConfig();
            if (m.getMetrics() == InterfaceName.TotalSpendOvertime
                    || m.getMetrics() == InterfaceName.AvgSpendOvertime) {
                m.getPeriodsConfig().set(0, withinToBetween(timeFilters.get(0)));
            }
        });
        return metrics;
    }

    private TimeFilter withinToBetween(TimeFilter timeFilter) {
        if (timeFilter.getRelation() != ComparisonType.WITHIN) {
            return timeFilter;
        }
        return TimeFilter.between(1, (int) timeFilter.getValues().get(0), timeFilter.getPeriod());
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

        // for action info: metrics display name without period
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
            String metricsNameWithPeriod = ActivityMetricsUtils.getNameWithPeriod(metrics);
            String metricsDisplayName = ActivityMetricsUtils.getMetricsDisplayName(metrics.getMetrics());
            if (existingMetrics.containsKey(metricsNameWithPeriod)) {
                metrics.setPid(existingMetrics.get(metricsNameWithPeriod).getPid());
                metrics.setCreated(existingMetrics.get(metricsNameWithPeriod).getCreated());
            }
            if (deactivated.contains(metricsDisplayName)) {
                deactivated.remove(metricsDisplayName);
                updated.add(metricsDisplayName);
            } else if (!updated.contains(metricsDisplayName)) {
                activated.add(metricsDisplayName);
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

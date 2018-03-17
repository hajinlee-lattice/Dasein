package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

@Service("activityMetricsService")
public class ActivityMetricsServiceImpl implements ActivityMetricsService {
    @Inject
    private ActivityMetricsEntityMgr entityMgr;

    @Override
    public List<ActivityMetrics> findActiveWithType(ActivityType type) {
        // return entityMgr.findActiveWithType(type);
        return fakeMetrics();
    }

    @Override
    public List<ActivityMetrics> save(ActivityType type, List<ActivityMetrics> metricsList) {
        metricsList.forEach(metrics -> {
            metrics.setType(type);
        });
        return entityMgr.save(metricsList);
    }

    // Serve the API before UI has finished metrics configuration for tenant
    private List<ActivityMetrics> fakeMetrics() {
        ActivityMetrics margin = fakeSingleMetrics();
        margin.setMetrics(InterfaceName.Margin);
        margin.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Week)));

        ActivityMetrics shareOfWallet = fakeSingleMetrics();
        shareOfWallet.setMetrics(InterfaceName.ShareOfWallet);
        shareOfWallet.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Month)));

        ActivityMetrics avgSpendOvertime = fakeSingleMetrics();
        avgSpendOvertime.setMetrics(InterfaceName.AvgSpendOvertime);
        avgSpendOvertime.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Quarter)));

        ActivityMetrics totalSpendOvertime = fakeSingleMetrics();
        totalSpendOvertime.setMetrics(InterfaceName.TotalSpendOvertime);
        totalSpendOvertime.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Year)));

        ActivityMetrics spendChange = fakeSingleMetrics();
        spendChange.setMetrics(InterfaceName.SpendChange);
        spendChange.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, Period.Month), TimeFilter.between(2, 3, Period.Month)));

        return Arrays.asList(margin, shareOfWallet, avgSpendOvertime, totalSpendOvertime, spendChange);
    }

    private ActivityMetrics fakeSingleMetrics() {
        Tenant tenant = MultiTenantContext.getTenant();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setType(ActivityType.PurchaseHistory);
        metrics.setTenant(tenant);
        metrics.setEOL(false);
        metrics.setDeprecated(null);
        metrics.setCreated(new Date());
        metrics.setUpdated(metrics.getCreated());
        return metrics;
    }
}

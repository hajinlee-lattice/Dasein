package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

/**
 * Fake data to test activity metrics
 *
 */
public final class ActivityMetricsTestUtils {

    protected ActivityMetricsTestUtils() {
        throw new UnsupportedOperationException();
    }

    public static List<ActivityMetrics> fakePurchaseMetrics(Tenant tenant) {
        ActivityMetrics margin = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        margin.setMetrics(InterfaceName.Margin);
        margin.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        ActivityMetrics shareOfWallet = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        shareOfWallet.setMetrics(InterfaceName.ShareOfWallet);
        shareOfWallet.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        ActivityMetrics spendChange = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        spendChange.setMetrics(InterfaceName.SpendChange);
        spendChange.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Quarter.name()),
                TimeFilter.between(2, 3, PeriodStrategy.Template.Quarter.name())));

        ActivityMetrics avgSpendOvertime = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        avgSpendOvertime.setMetrics(InterfaceName.AvgSpendOvertime);
        avgSpendOvertime.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Quarter.name())));

        return Arrays.asList(margin, shareOfWallet, spendChange, avgSpendOvertime);
    }

    public static ActivityMetrics createPurchaseMetrics(Tenant tenant, InterfaceName metrics,
            PeriodStrategy.Template periodTemplate, List<Integer> periods) {
        ActivityMetrics am = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        am.setMetrics(metrics);
        switch (metrics) {
        case HasPurchased:
            am.setPeriodsConfig(Arrays.asList(TimeFilter.ever()));
            break;
        case Margin:
        case ShareOfWallet:
        case TotalSpendOvertime:
        case AvgSpendOvertime:
            am.setPeriodsConfig(Arrays.asList(TimeFilter.within(periods.get(0), periodTemplate.name())));
            break;
        case SpendChange:
            am.setPeriodsConfig(Arrays.asList(TimeFilter.within(periods.get(0), periodTemplate.name()),
                    TimeFilter.between(periods.get(1), periods.get(2), periodTemplate.name())));
            break;
        default:
            throw new IllegalArgumentException("Unrecognized purchase metrics " + metrics);
        }
        return am;
    }

    public static List<ActivityMetrics> fakeUpdatedPurchaseMetrics(Tenant tenant) {
        ActivityMetrics totalSpendOvertime = createFakedMetrics(tenant, ActivityType.PurchaseHistory);
        totalSpendOvertime.setMetrics(InterfaceName.TotalSpendOvertime);
        totalSpendOvertime.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Year.name())));

        return Arrays.asList(totalSpendOvertime);
    }

    private static ActivityMetrics createFakedMetrics(Tenant tenant, ActivityType type) {
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setType(type);
        metrics.setTenant(tenant);
        metrics.setEOL(false);
        metrics.setDeprecated(null);
        metrics.setCreated(new Date());
        metrics.setUpdated(metrics.getCreated());
        return metrics;
    }
}

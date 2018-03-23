package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class ActivityMetricsUtilsTestNG {

    @Test(groups = "unit", dataProvider = "Metrics")
    public void test(String fullName, String activityId, InterfaceName metrics, String displayName, String periods) {
        Assert.assertEquals(ActivityMetricsUtils.getActivityIdFromFullName(fullName), activityId);
        Assert.assertEquals(ActivityMetricsUtils.getMetricsFromFullName(fullName), metrics);
        Assert.assertEquals(ActivityMetricsUtils.getPeriodsFromFullName(fullName), periods);

        List<PeriodStrategy> strategies = PeriodStrategy.NATURAL_PERIODS;
        Assert.assertEquals(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-01-01", strategies).getLeft(),
                displayName);
        System.out.println(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-12-31", strategies).getRight());
    }

    // full name, activity id, metrics, display name, period str
    @DataProvider(name = "Metrics")
    protected Object[][] provideMetrics() {
        return new Object[][] {
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HP", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.HasPurchased, "Has Purchased", "EVER" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_1__SW", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.ShareOfWallet, "% share of wallet in last 1 month", "M_1" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_2__M_3_5__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% spend change in last 2 months", "M_2__M_3_5" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_3_5__M_2__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% spend change in last 2 months", "M_3_5__M_2" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Y_10__MG", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.Margin, "% margin in last 10 years", "Y_10" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__W_10__TS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.TotalSpendOvertime, "Total spend in last 10 weeks", "W_10" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Q_1__AS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.AvgSpendOvertime, "Average spend in last 1 quarter", "Q_1" }, //
        };
    }

}

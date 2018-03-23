package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class ActivityMetricsUtilsTestNG {

    @Test(groups = "unit", dataProvider = "Metrics")
    public void test(String fullName, String activityId, InterfaceName metrics, String displayName, String periods,
            String depivotedName, String secDisplayName) {
        Assert.assertEquals(ActivityMetricsUtils.getActivityIdFromFullName(fullName), activityId);
        Assert.assertEquals(ActivityMetricsUtils.getMetricsFromFullName(fullName), metrics);
        Assert.assertEquals(ActivityMetricsUtils.getPeriodsFromFullName(fullName), periods);

        List<PeriodStrategy> strategies = PeriodStrategy.NATURAL_PERIODS;
        Assert.assertEquals(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-01-01", strategies).getLeft(),
                displayName);
        Assert.assertEquals(ActivityMetricsUtils.getDepivotedAttrNameFromFullName(fullName), depivotedName);
        Assert.assertEquals(ActivityMetricsUtils.getDepivotedAttrNameFromFullName(fullName), depivotedName);
        Assert.assertEquals(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-01-01", strategies).getRight(),
                secDisplayName);
    }

    // full name, activity id, metrics, display name, period str
    @DataProvider(name = "Metrics")
    protected Object[][] provideMetrics() {
        return new Object[][] {
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HP", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.HasPurchased, "Has Purchased", "EVER", "EVER__HP", null }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_1__SW", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.ShareOfWallet, "% share of wallet in last 1 month", "M_1", "M_1__SW",
                        "(2017-12-01 to 2017-12-31)" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_2__M_3_5__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% spend change in last 2 months", "M_2__M_3_5", "M_2__M_3_5__SC",
                        "(2017-11-01 to 2017-12-31)" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_3_5__M_2__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% spend change in last 2 months", "M_3_5__M_2", "M_3_5__M_2__SC",
                        "(2017-11-01 to 2017-12-31)" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Y_10__MG", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.Margin, "% margin in last 10 years", "Y_10", "Y_10__MG",
                        "(2008-01-01 to 2017-12-31)" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__W_10__TS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.TotalSpendOvertime, "Total spend in last 10 weeks", "W_10", "W_10__TS",
                        "(2017-10-22 to 2017-12-30)" }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Q_1__AS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.AvgSpendOvertime, "Average spend in last 1 quarter", "Q_1", "Q_1__AS",
                        "(2017-10-01 to 2017-12-31)" }, //
        };
    }

}

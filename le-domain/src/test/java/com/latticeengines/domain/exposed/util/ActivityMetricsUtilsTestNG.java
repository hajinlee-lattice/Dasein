package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ActivityMetricsUtilsTestNG {
    @Test(groups = "unit")
    public void testGetActivityIdFromFullName() {
        String fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HasPurchased";
        Assert.assertEquals(ActivityMetricsUtils.getActivityIdFromFullName(fullName),
                "FE5FB1286A4E60345D0E4AAD0E66E664");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__Margin";
        Assert.assertEquals(ActivityMetricsUtils.getActivityIdFromFullName(fullName),
                "FE5FB1286A4E60345D0E4AAD0E66E664");
    }

    @Test(groups = "unit")
    public void testGetMetricsFromFullName() {
        String fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HasPurchased";
        Assert.assertEquals(ActivityMetricsUtils.getMetricsFromFullName(fullName), "HasPurchased");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__Margin";
        Assert.assertEquals(ActivityMetricsUtils.getMetricsFromFullName(fullName), "Margin");
    }

    @Test(groups = "unit")
    public void testGetPeriodsFromFullName() {
        String fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HasPurchased";
        Assert.assertEquals(ActivityMetricsUtils.getPeriodsFromFullName(fullName), "EVER");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__Margin";
        Assert.assertEquals(ActivityMetricsUtils.getPeriodsFromFullName(fullName), "WITHIN_Month_1");
    }

    @Test(groups = "unit")
    public void testGetDisplayNamesFromFullName() {
        String fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HasPurchased";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(), "Has Purchased");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__ShareOfWallet";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(),
                "Percentage share of wallet in last 1 month");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_2__SpendChange";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(),
                "Percentage spend change in last 2 months");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_10__Margin";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(),
                "Percentage margin in last 10 months");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__BETWEEN_Month_2_3__AvgSpendOvertime";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(),
                "Average spend in last 1 month");
        fullName = "AM_FE5FB1286A4E60345D0E4AAD0E66E664__WITHIN_Month_1__BETWEEN_Month_2_3__TotalSpendOvertime";
        Assert.assertEquals(ActivityMetricsUtils.getDisplayNamesFromFullName(fullName).getLeft(),
                "Total spend in last 1 month");
    }
}

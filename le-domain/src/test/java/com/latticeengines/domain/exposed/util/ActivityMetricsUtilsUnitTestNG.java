package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "MetricsToTestName")
    public void testVariousGetNames(String fullName, String productId, InterfaceName metrics, String displayName,
            String periods, String depivotedName, String secDisplayName, NullMetricsImputation nullImputation) {
        Assert.assertEquals(ActivityMetricsUtils.getProductIdFromFullName(fullName), productId);
        Assert.assertEquals(ActivityMetricsUtils.getMetricsFromFullName(fullName), metrics);
        Assert.assertEquals(ActivityMetricsUtils.getPeriodsFromFullName(fullName), periods);

        List<PeriodStrategy> strategies = PeriodStrategy.NATURAL_PERIODS;
        Assert.assertEquals(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-01-01", strategies).getLeft(),
                displayName);
        Assert.assertEquals(ActivityMetricsUtils.getNameWithPeriodFromFullName(fullName), depivotedName);
        Assert.assertEquals(ActivityMetricsUtils.getNameWithPeriodFromFullName(fullName), depivotedName);
        Assert.assertEquals(
                ActivityMetricsUtils.getDisplayNamesFromFullName(fullName, "2018-01-01", strategies).getRight(),
                secDisplayName);
        Assert.assertEquals(ActivityMetricsUtils.getNullImputation(fullName), nullImputation);
    }

    // full name, product id, metrics, display name, period str, depivoted metrics name, secondary display name, null imputation
    @DataProvider(name = "MetricsToTestName")
    protected Object[][] provideMetricsToTestName() {
        return new Object[][] {
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__EVER__HP", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.HasPurchased, "Has Purchased", "EVER", "EVER__HP", null,
                        NullMetricsImputation.FALSE }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_1__SW", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.ShareOfWallet, "% Share of Wallet in last 1 month", "M_1", "M_1__SW",
                        "(2017-12-01 to 2017-12-31)", NullMetricsImputation.NULL }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_2__M_3_5__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% Spend Change in last 2 months", "M_2__M_3_5", "M_2__M_3_5__SC",
                        "(2017-11-01 to 2017-12-31)", NullMetricsImputation.ZERO }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_3_5__M_2__SC", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.SpendChange, "% Spend Change in last 2 months", "M_3_5__M_2", "M_3_5__M_2__SC",
                        "(2017-11-01 to 2017-12-31)", NullMetricsImputation.ZERO }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Y_10__MG", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.Margin, "% Margin in last 10 years", "Y_10", "Y_10__MG",
                        "(2008-01-01 to 2017-12-31)", NullMetricsImputation.NULL }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__W_10__TS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.TotalSpendOvertime, "Total Spend in last 10 weeks", "W_10", "W_10__TS",
                        "(2017-10-22 to 2017-12-30)", NullMetricsImputation.ZERO }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__M_2_3__TS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.TotalSpendOvertime, "Total Spend in last 2 to 3 months", "M_2_3", "M_2_3__TS",
                        "(2017-10-01 to 2017-11-30)", NullMetricsImputation.ZERO }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Q_1__AS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.AvgSpendOvertime, "Average Spend in last 1 quarter", "Q_1", "Q_1__AS",
                        "(2017-10-01 to 2017-12-31)", NullMetricsImputation.ZERO }, //
                { "AM_FE5FB1286A4E60345D0E4AAD0E66E664__Y_1_1__AS", "FE5FB1286A4E60345D0E4AAD0E66E664",
                        InterfaceName.AvgSpendOvertime, "Average Spend in last 1 to 1 years", "Y_1_1", "Y_1_1__AS",
                        "(2017-01-01 to 2017-12-31)", NullMetricsImputation.ZERO }, //
        };
    }

    @Test(groups = "unit")
    public void testIsDeprecated() {
        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        List<ActivityMetrics> metrics = ActivityMetricsTestUtils.fakePurchaseMetrics(tenant);
        for (ActivityMetrics m : metrics) {
            if (m.getMetrics() == InterfaceName.Margin) {
                m.setEOL(true);
            }
        }
        for (ActivityMetrics m : metrics) {
            String fullName = ActivityMetricsUtils.getFullName(m, "FE5FB1286A4E60345D0E4AAD0E66E664");
            if (m.getMetrics() == InterfaceName.Margin) {
                Assert.assertTrue(ActivityMetricsUtils.isDeprecated(fullName, metrics));
            } else {
                Assert.assertFalse(ActivityMetricsUtils.isDeprecated(fullName, metrics));
            }
        }
    }

    @Test(groups = "unit")
    public void testIsValidCrossMetrics() {
        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        List<ActivityMetrics> ams = new ArrayList<>();
        Assert.assertTrue(ActivityMetricsUtils.isValidMetrics(ams));

        // Verify allowed maximum counts for each type of metrics
        ActivityMetricsUtils.MAX_CNTS.forEach((metrics, cnt) -> {
            ams.clear();
            // within maximum limit
            IntStream.range(0, cnt).forEach(i -> ams.add(ActivityMetricsTestUtils.createPurchaseMetrics(tenant,
                    metrics,
                    PeriodStrategy.Template.Week, Arrays.asList(i + 1, i + 1, i + 1))));
            Assert.assertTrue(ActivityMetricsUtils.isValidMetrics(ams));

            ams.clear();
            // exceed maximum limit
            IntStream.range(0, cnt + 1).forEach(i -> ams.add(ActivityMetricsTestUtils.createPurchaseMetrics(tenant,
                    metrics, PeriodStrategy.Template.Week, Arrays.asList(i + 1, i + 1, i + 1))));
            Assert.assertThrows(LedpException.class, () -> ActivityMetricsUtils.isValidMetrics(ams));
        });

        // Verify duplicate config of metrics
        ActivityMetricsUtils.MAX_CNTS.forEach((metrics, cnt) -> {
            if (cnt <= 1) {
                return;
            }
            ams.clear();
            // within maximum limit but period is same
            IntStream.range(0, cnt).forEach(i -> ams.add(ActivityMetricsTestUtils.createPurchaseMetrics(tenant, metrics,
                    PeriodStrategy.Template.Week, Arrays.asList(1, 2, 3))));
            Assert.assertThrows(LedpException.class, () -> ActivityMetricsUtils.isValidMetrics(ams));
        });
    }

}

package com.latticeengines.domain.exposed.cdl.scheduling;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchedulingPAUtilsUnitTestNG {

    private static final LocalDate TEST_DATE = LocalDate.of(2020, 8, 14);

    @Test(groups = "unit", dataProvider = "peacePeriod")
    private void testPeachPeriod(PeacePeriodTestCase testCase) {
        Pair<Instant, Instant> period = SchedulingPAUtils.calculatePeacePeriod(testCase.now, testCase.timezone,
                testCase.autoScheduleQuota);
        Assert.assertEquals(period, testCase.expectedPeacePeriod);
        Assert.assertEquals(
                SchedulingPAUtils.isInPeacePeriod(testCase.now, testCase.timezone, testCase.autoScheduleQuota),
                testCase.isInPeacePeriod);
    }

    @DataProvider(name = "peacePeriod")
    private Object[][] peacePeriodTestData() {
        return new Object[][] { //
                { newPeacePeriodTestCase(3, "Europe/Warsaw", 1, new int[] { 6, 18 }, false) }, //
                { newPeacePeriodTestCase(6, "Europe/Warsaw", 1, new int[] { 6, 18 }, true) }, //
                { newPeacePeriodTestCase(12, "America/Marigot", 1, new int[] { 6, 18 }, true) }, //
                { newPeacePeriodTestCase(14, "UTC", 1, new int[] { 6, 18 }, true) }, //
                { newPeacePeriodTestCase(19, "GMT", 1, new int[] { 6, 18 }, false) }, //
                /*-
                 * for every quota more than 1, period is decreased by 4 hr, align with start time
                 */
                { newPeacePeriodTestCase(15, "America/Los_Angeles", 2, new int[] { 6, 14 }, false) }, //
                { newPeacePeriodTestCase(12, "US/Hawaii", 2, new int[] { 6, 14 }, true) }, //
                { newPeacePeriodTestCase(5, "US/Hawaii", 2, new int[] { 6, 14 }, false) }, //
                { newPeacePeriodTestCase(9, "US/Hawaii", 3, new int[] { 6, 10 }, true) }, //
                { newPeacePeriodTestCase(5, "UTC", 3, new int[] { 6, 10 }, false) }, //
                { newPeacePeriodTestCase(11, "UTC", 3, new int[] { 6, 10 }, false) }, //
                { newPeacePeriodTestCase(9, "US/Hawaii", 4, null, false) }, //
                { newPeacePeriodTestCase(6, "UTC", 4, null, false) }, //
                { newPeacePeriodTestCase(12, "UTC", 4, null, false) }, //
                { newPeacePeriodTestCase(12, "America/Los_Angeles", 5, null, false) }, //
                { newPeacePeriodTestCase(12, "America/Los_Angeles", 12, null, false) }, //
        }; //
    }

    private PeacePeriodTestCase newPeacePeriodTestCase(int nowInHr, String timezoneStr, int autoScheduleQuota,
            int[] expectedPeacePeriodInHr, boolean isInPeacePeriod) {
        PeacePeriodTestCase testCase = new PeacePeriodTestCase();
        ZoneId timezone = ZoneId.of(timezoneStr);
        testCase.now = TEST_DATE.atTime(nowInHr, 0).atZone(timezone).toInstant();
        if (expectedPeacePeriodInHr != null) {
            Instant start = TEST_DATE.atTime(expectedPeacePeriodInHr[0], 0).atZone(timezone).toInstant();
            Instant end = TEST_DATE.atTime(expectedPeacePeriodInHr[1], 0).atZone(timezone).toInstant();
            testCase.expectedPeacePeriod = Pair.of(start, end);
        }
        testCase.timezone = timezone;
        testCase.autoScheduleQuota = autoScheduleQuota;
        testCase.isInPeacePeriod = isInPeacePeriod;
        return testCase;
    }

    private static class PeacePeriodTestCase {
        Instant now;
        ZoneId timezone;
        long autoScheduleQuota;
        Pair<Instant, Instant> expectedPeacePeriod;
        boolean isInPeacePeriod;
    }
}

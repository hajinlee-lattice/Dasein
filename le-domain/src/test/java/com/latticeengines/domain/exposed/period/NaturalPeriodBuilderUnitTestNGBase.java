package com.latticeengines.domain.exposed.period;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class NaturalPeriodBuilderUnitTestNGBase {

    static final String PERIOD_ID_DATA_PROVIDER = "periodCountDtaProvider";
    static final String PERIOD_RANGE_DATA_PROVIDER = "periodRangeDtaProvider";

    @Test(groups = "unit")
    public void testPeriodIntegrity() {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
        PeriodBuilder periodBuilder = getBuilder();
        LocalDate previousEnd = null;
        for (int i = -100; i < 100; i++) {
            Pair<LocalDate, LocalDate> dateRange = periodBuilder.toDateRange(i, i);
            LocalDate start = dateRange.getLeft();
            LocalDate end = dateRange.getRight();
            Assert.assertNotNull(start);
            Assert.assertNotNull(end);
            String startStr = start.format(formatter);
            String endStr = end.format(formatter);
            Assert.assertEquals(periodBuilder.toPeriodId(startStr), i,
                    String.format("Date %s is parsed to period %d, while it should be %d", startStr,
                            periodBuilder.toPeriodId(startStr), i));
            Assert.assertEquals(periodBuilder.toPeriodId(endStr), i,
                    String.format("Date %s is parsed to period %d, while it should be %d", endStr,
                            periodBuilder.toPeriodId(endStr), i));
            if (previousEnd != null) {
                long dayDiff = Duration.between(previousEnd.atStartOfDay(), start.atStartOfDay()).toDays();
                Assert.assertEquals(dayDiff, 1, String.format("Period %d ends at %s, but period %d starts at %s", i - 1,
                        previousEnd, i, start));
            }
            previousEnd = end;
        }
    }

    @Test(groups = "unit", dataProvider = PERIOD_RANGE_DATA_PROVIDER)
    public void testConvertToDateRange(int period1, int period2, String firstDate, String startDate, String endDate) {
        PeriodBuilder builder = StringUtils.isBlank(firstDate) ? getBuilder() : getBuilder(firstDate);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(period1, period2);
        Assert.assertEquals(dateRange.getLeft(), LocalDate.parse(startDate));
        Assert.assertEquals(dateRange.getRight(), LocalDate.parse(endDate));
    }

    @Test(groups = "unit", dataProvider = PERIOD_ID_DATA_PROVIDER)
    public void testConvertToPeriodId(String startDate, String endDate, int period) {
        PeriodBuilder builder = StringUtils.isBlank(startDate) ? getBuilder() : getBuilder(startDate);
        int actualPeriod = builder.toPeriodId(endDate);
        Assert.assertEquals(new Integer(actualPeriod), new Integer(period));
    }

    protected abstract PeriodBuilder getBuilder();
    protected abstract PeriodBuilder getBuilder(String startDate);

}

package com.latticeengines.domain.exposed.period;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public abstract class BusinessPeriodBuilderUnitTestNGBase {

    static final String BUSINESS_CALENDAR_PROVIDER = "businessCalendarProvider";
    static final String SPECIAL_DATE_PROVIDER = "dateProvider";

    @Test(groups = "unit", dataProvider = BUSINESS_CALENDAR_PROVIDER)
    public void testPeriodIntegrity(BusinessCalendar.Mode mode, String startingDate, String startingDay, int longerMonth) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
        BusinessCalendar calendar = createBusinessCalendar(mode, startingDate, startingDay, longerMonth);
        PeriodBuilder periodBuilder = getBuilder(calendar);
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
                    String.format("Date %s is parsed to period %d, while it should be %d, with BusinessCalendar %s",
                            startStr, periodBuilder.toPeriodId(startStr), i, JsonUtils.serialize(calendar)));
            Assert.assertEquals(periodBuilder.toPeriodId(endStr), i,
                    String.format("Date %s is parsed to period %d, while it should be %d, with BusinessCalendar %s",
                            endStr, periodBuilder.toPeriodId(endStr), i, JsonUtils.serialize(calendar)));
            if (previousEnd != null) {
                long dayDiff = Duration.between(previousEnd.atStartOfDay(), start.atStartOfDay()).toDays();
                Assert.assertEquals(dayDiff, 1,
                        String.format("Period %d ends at %s, but period %d starts at %s, with BusinessCalendar %s",
                                i - 1, previousEnd, i, start, JsonUtils.serialize(calendar)));
            }
            previousEnd = end;
        }
    }

    @Test(groups = "unit", dataProvider = SPECIAL_DATE_PROVIDER)
    public void testPeriodIntegrity(BusinessCalendar.Mode mode, String startingDate, String startingDay,
            int longerMonth, int startYear, String date, int periodId) {
        BusinessCalendar calendar = createBusinessCalendar(mode, startingDate, startingDay, longerMonth);
        PeriodBuilder periodBuilder = getBuilder(calendar, startYear);
        Assert.assertEquals(periodBuilder.toPeriodId(date), periodId);
    }


    private BusinessCalendar createBusinessCalendar(BusinessCalendar.Mode mode, String startingDate, String startingDay,
            int longerMonth) {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(mode);
        calendar.setLongerMonth(longerMonth);
        calendar.setStartingDate(startingDate);
        calendar.setStartingDay(startingDay);
        return calendar;
    }

    protected abstract PeriodBuilder getBuilder(BusinessCalendar calendar);

    protected abstract PeriodBuilder getBuilder(BusinessCalendar calendar, int startYear);

    // Schema: Mode, StartingDate, StartingDay, LongerMonth
    @DataProvider(name = BUSINESS_CALENDAR_PROVIDER)
    protected Object[][] provideBusinessCalendarParas() {
        return new Object[][] { //
                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 0 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-31", null, 0 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "APR-15", null, 0 }, //

                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 1 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-31", null, 1 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "MAY-15", null, 1 }, //

                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 2 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-31", null, 2 }, //
                { BusinessCalendar.Mode.STARTING_DATE, "JUN-15", null, 2 }, //

                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-JAN", 0 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "4th-FRI-DEC", 0 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "3rd-WED-APR", 0 }, //

                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-JAN", 1 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "4th-FRI-DEC", 1 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "3rd-WED-MAY", 1 }, //

                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-JAN", 2 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "4th-FRI-DEC", 2 }, //
                { BusinessCalendar.Mode.STARTING_DAY, null, "3rd-WED-JUN", 2 }, //
        };
    }
}

package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessYearPeriodBuilderUnitTestNG extends BusinessPeriodBuilderUnitTestNGBase {
    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar) {
        return new BusinessYearPeriodBuilder(calendar);
    }

    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar, int startYear) {
        return new BusinessYearPeriodBuilder(calendar, startYear);
    }

    // Schema: Mode, StartingDate, StartingDay, LongerMonth, startYear, date,
    // periodId
    @DataProvider(name = SPECIAL_DATE_PROVIDER)
    protected Object[][] provideDate() {
        return new Object[][] {
                // 2016 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-28", null, 0, 2016, "2016-02-29", 0 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-28", null, 0, 2016, "2016-02-27", -1 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-28", null, 0, 2016, "2017-02-27", 0 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-28", null, 0, 2016, "2017-02-28", 1 },
                // 2015 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2016-02-29", -1 },
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2016-03-01", 0 },
                // Test special date
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-31", null, 0, 2016, "2017-12-30", 0 },
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-31", null, 0, 2016, "2017-12-31", 1 },
                // 2017 business year has 53 weeks 2017.1.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2017-01-01", 0 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2018-01-06", 0 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2018-01-07", 1 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-12-31", -1 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-01-03", -1 }, // 2016.1.3 is 1st SUN in JAN
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-01-02", -2 }, // 2016.1.3 is 1st SUN in JAN

        };
    }
}

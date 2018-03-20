package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessQuarterPeriodBuilderUnitTestNG extends BusinessPeriodBuilderUnitTestNGBase {
    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar) {
        return new BusinessQuarterPeriodBuilder(calendar);
    }

    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar, int startYear) {
        return new BusinessQuarterPeriodBuilder(calendar, startYear);
    }

    // Schema: Mode, StartingDate, StartingDay, LongerMonth, startYear, date, periodId
    @DataProvider(name = SPECIAL_DATE_PROVIDER)
    protected Object[][] provideDate() {
        return new Object[][] {
                // 2015 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "DEC-01", null, 0, 2015, "2016-02-29", 0 }, // 0st quarter 2015.12.1 - 2016.2.29
                { BusinessCalendar.Mode.STARTING_DATE, "NOV-30", null, 0, 2015, "2016-02-29", 1 }, // 0st quarter 2015.11.30 - 2016.2.28
                // Test last special days
                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 0, 2017, "2017-12-31", 3 },
                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 0, 2017, "2018-01-01", 4 },
                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 0, 2017, "2016-01-01", -4 },
                { BusinessCalendar.Mode.STARTING_DATE, "JAN-01", null, 0, 2017, "2016-12-31", -1 },
                // 2017 business year has 53 weeks 2017.1.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2018-01-06", 3 }, // last quarter has 14 weeks 2017.10.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2017-10-01", 3 }, // last quarter has 14 weeks 2017.10.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2017-09-30", 2 }, // last quarter has 14 weeks 2017.10.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-12-31", -1 }, // last quarter has 13 weeks 2016.10.1 - 2016.12.31
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-10-02", -1 }, // last quarter has 13 weeks 2016.10.2 - 2016.12.31
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-SUN-JAN", 0, 2017, "2016-10-01", -2 }, // last quarter has 13 weeks 2016.10.2 - 2016.12.31
        };
    }
}

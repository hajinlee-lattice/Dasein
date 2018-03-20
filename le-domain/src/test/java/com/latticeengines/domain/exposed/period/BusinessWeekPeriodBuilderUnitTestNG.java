package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessWeekPeriodBuilderUnitTestNG extends BusinessPeriodBuilderUnitTestNGBase {

    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar) {
        return new BusinessWeekPeriodBuilder(calendar);
    }

    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar, int startYear) {
        return new BusinessWeekPeriodBuilder(calendar, startYear);
    }

    // Schema: Mode, StartingDate, StartingDay, LongerMonth, startYear, date, periodId
    @DataProvider(name = SPECIAL_DATE_PROVIDER)
    protected Object[][] provideDate() {
        return new Object[][] {
                // Test leap year
                // 2016 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", null, 0, 2016, "2016-02-29", 3 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", null, 0, 2016, "2016-03-01", 4 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", null, 0, 2016, "2017-02-01", 51 },
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", null, 0, 2016, "2017-02-02", 52 },
                // 2015 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2016-02-29", -1 },
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2016-02-21", -1 }, // last week has 9 days
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2016-02-20", -2 }, // last week has 9 days
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", null, 0, 2016, "2015-02-28", -53 },
                // 2016 is leap business year
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-FEB", 0, 2016, "2016-02-01", 0 }, // 2016.02.02 is 1st Mon of Feb
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-FEB", 0, 2016, "2016-02-29", 4 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-FEB", 0, 2016, "2016-01-31", -1 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-FEB", 0, 2016, "2017-02-05", 52 }, // 2017.02.06 is 1st Mon of Feb
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-MON-FEB", 0, 2016, "2017-02-06", 53 }, // 2017.02.06 is 1st Mon of Feb
                // 2015 is leap business year
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-TUE-MAR", 0, 2016, "2016-02-29", -1 },
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-TUE-MAR", 0, 2016, "2015-03-03", -52 }, // 2015.03.03 is 1st Tue of March
                { BusinessCalendar.Mode.STARTING_DAY, null, "1st-TUE-MAR", 0, 2016, "2015-03-02", -53 }, // 2015.03.03 is 1st Tue of March
        };
    }
}

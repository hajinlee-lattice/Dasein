package com.latticeengines.domain.exposed.period;

import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessMonthPeriodBuilderUnitTestNG extends BusinessPeriodBuilderUnitTestNGBase {
    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar) {
        return new BusinessMonthPeriodBuilder(calendar);
    }

    @Override
    protected PeriodBuilder getBuilder(BusinessCalendar calendar, int startYear) {
        return new BusinessMonthPeriodBuilder(calendar, startYear);
    }
    
    // Schema: Mode, StartingFrom, LongerMonth, startYear, date, periodId
    @DataProvider(name = SPECIAL_DATE_PROVIDER)
    protected Object[][] provideDate() {
        return new Object[][] {
                // Test longer month
                // 2016 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 1, 2016, "2016-02-29", 0 }, // 2016 1st month: 2.2 - 3.7 5 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 1, 2016, "2016-03-07", 0 }, // 2016 1st month: 2.2 - 3.7 5 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 1, 2016, "2016-03-08", 1 }, // 2016 1st month: 2.2 - 3.7 5 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 2, 2016, "2016-02-29", 0 }, // 2016 1st month: 2.2 - 2.29 4 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 2, 2016, "2016-03-01", 1 }, // 2016 2nd month: 3.1 - 4.4 5 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 2, 2016, "2016-04-04", 1 }, // 2016 2nd month: 3.1 - 4.4 5 weeks
                { BusinessCalendar.Mode.STARTING_DATE, "FEB-02", 2, 2016, "2016-04-05", 2 }, // 2016 2nd month: 3.1 - 4.4 5 weeks
                // 2015 is leap business year
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", 3, 2016, "2016-02-29", -1 }, // 2015 4th month: 1.24 - 2.29 5 weeks + 2 days
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", 3, 2016, "2016-01-24", -1 }, // 2015 4th month: 1.24 - 2.29 5 weeks + 2 days
                { BusinessCalendar.Mode.STARTING_DATE, "MAR-01", 3, 2016, "2016-01-23", -2 }, // 2015 4th month: 1.24 - 2.29 5 weeks + 2 days
                // 2017 business year has 53 weeks 2017.1.1 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 1, 2017, "2018-01-06", 11 }, // last month 5 weeks 2017.12.03 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 1, 2017, "2017-12-03", 11 }, // last month 5 weeks 2017.12.03 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 1, 2017, "2017-12-02", 10 }, // last month 5 weeks 2017.12.03 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 3, 2017, "2017-11-26", 11 }, // last month 6 weeks 2017.11.26 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 3, 2017, "2017-11-25", 10 }, // last month 6 weeks 2017.11.26 - 2018.1.6
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 1, 2017, "2016-12-04", -1 }, // last month 4 weeks 2016.12.4 - 2016.12.31
                { BusinessCalendar.Mode.STARTING_DAY, "1st-SUN-JAN", 1, 2017, "2016-12-03", -2 }, // last month 4 weeks 2016.12.4 - 2016.12.31
        };
    }
}

package com.latticeengines.domain.exposed.util;

import java.time.LocalDate;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessCalendarUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "startingDateProvider")
    public void testValidateStartingDate(String startingDate, Integer evaluationYear, String expectedNote, LedpCode ledpCode) {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate(startingDate);
        calendar.setEvaluationYear(evaluationYear);
        calendar.setLongerMonth(1);
        verifyValidation(calendar, expectedNote, ledpCode);
    }

    @DataProvider(name = "startingDateProvider")
    public Object[][] provideStartingDate() {
        LocalDate now = LocalDate.now();
        int year = now.getYear();
        int daysInLastWeek = now.isLeapYear() ? 9 : 8;
        String msg = String.format("The last week of Fiscal Year **%d** will have **%d** days.", year, daysInLastWeek);
        return new Object[][]{ //
                { "JAN-01", null, msg, null }, //
                { "JAN-15", null, msg, null }, //
                { "DEC-01", null, msg, null }, //
                { "JAN-05", 2016, "The last week of Fiscal Year **2016** will have **9** days.", null }, //
                { "JAN-32", null, null, LedpCode.LEDP_40015 }, //
                { "FEB-29", null, null, LedpCode.LEDP_40015 }, //
                { "FEB-29", 2016, null, LedpCode.LEDP_40015 }, //
        };
    }

    @Test(groups = "unit", dataProvider = "startingDayProvider")
    public void testValidateStartingDay(String startingDay, Integer evaluationYear, String expectedNote, LedpCode ledpCode) {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DAY);
        calendar.setStartingDay(startingDay);
        calendar.setEvaluationYear(evaluationYear);
        calendar.setLongerMonth(1);
        verifyValidation(calendar, expectedNote, ledpCode);
    }

    @DataProvider(name = "startingDayProvider")
    public Object[][] provideStartingDay() {
        return new Object[][]{ //
                { "5th-MON-JAN", 2016, null, LedpCode.LEDP_40015 }, //
                { "1st-MON-JAN", 2016,
                        "The Fiscal Year **2016** has **52** weeks, **04 January 2016** to **01 January 2017**.",
                        null }, //
                { "1st-FRI-JAN", 2016,
                        "The Fiscal Year **2016** has **53** weeks, **01 January 2016** to **05 January 2017**.",
                        null }, //
                { "1st-MON-JAN", 2017,
                        "The Fiscal Year **2017** has **52** weeks, **02 January 2017** to **31 December 2017**.",
                        null }, //
                { "4th-FRI-DEC", 2017,
                        "The Fiscal Year **2017** has **53** weeks, **22 December 2017** to **27 December 2018**.",
                        null }, //
        };
    }

    private void verifyValidation(BusinessCalendar calendar, String expectedNote, LedpCode ledpCode) {
        if (ledpCode != null) {
            boolean hasException = false;
            try {
                BusinessCalendarUtils.validate(calendar);
            } catch (LedpException e) {
                Assert.assertEquals(e.getCode(), ledpCode);
                hasException = true;
            }
            Assert.assertTrue(hasException);
        } else {
            String note = BusinessCalendarUtils.validate(calendar);
            Assert.assertEquals(note, expectedNote);
        }
    }

}

package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PhoneNumberUtilsUnitTestNG {

    @DataProvider(name = "phoneDataProvider")
    Object[][] phoneDataProvider() {
        return new Object[][] { //
                { "US", "(877) 460-0010", "18774600010" }, //
                { "US", "+18774600010", "18774600010" }, //
                { "US", "8774600010", "18774600010" }, //
                { "US", "877 460 0010", "18774600010" }, //
                { "CN", "86-13520598855", "8613520598855" }, //
                { "CN", "(86)13520598855", "8613520598855" }, //
                { "CN", "(86)01065052257", "861065052257" }, //
                { "CN", "86 010 65052257", "861065052257" }, //
                { "CN", "01065052257", "861065052257" }, //
                { "", "+13453332221", "13453332221" }, //
                { "", "(444) 123-3242", "14441233242" }
        };
    }

    @Test(groups = "unit", dataProvider = "phoneDataProvider")
    public void testStandardizePhone(String countryCode, String input, String expectedOutput) {
        String output = PhoneNumberUtils.getStandardPhoneNumber(input, countryCode);
        Assert.assertEquals(output, expectedOutput);
    }

}

package com.latticeengines.domain.exposed.util;

import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ActivityStoreUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "modifyPatternTestData")
    private void testModifyPattern(String ptn, String expectedModifiedPtn, boolean isValidPtn) {
        String regexStr = ActivityStoreUtils.modifyPattern(ptn);
        Assert.assertEquals(regexStr, expectedModifiedPtn);

        Assert.assertEquals(isValidRegex(regexStr), isValidPtn,
                "Pattern: " + ptn + " Modified Pattern: " + expectedModifiedPtn
                        + " doesn't have the expected validity as regex");
    }

    private boolean isValidRegex(String regexStr) {
        try {
            Pattern.compile(regexStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @DataProvider(name = "modifyPatternTestData")
    private Object[][] modifyPatternTestData() {
        return new Object[][] { //
                { "", "", true }, //
                { "   ", "   ", true }, //
                { "*.**", ".*.*.*", true }, //
                { "https://google.com/*", "https://google.com/.*", true }, //
                { "*google.com/about/*", ".*google.com/about/.*", true }, //
                { "https://google.com/.*/*.html", "https://google.com/.*/.*.html", true }, //
                { "https://google.com/.*/*\\.html", "https://google.com/.*/.*\\.html", true }, // escape . to be safer
                /* invalid pattern */
                { null, null, false }, //
                { "[", "[", false }, //
                { "*[*", ".*[.*", false }, //
        };
    }
}

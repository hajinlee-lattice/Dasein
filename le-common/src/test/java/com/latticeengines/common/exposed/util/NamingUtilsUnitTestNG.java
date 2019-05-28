package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NamingUtilsUnitTestNG {

    @Test(groups = "unit")
    public void timestampWithRandom() {
        for (int i = 0; i < 5; i++) {
            String name = NamingUtils.timestampWithRandom("MatchDataCloud");
            Pattern pattern = Pattern.compile(
                    "MatchDataCloud_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{3}_[A-Z]{3}_[0-9]{1,10}");
            Matcher matcher = pattern.matcher(name);
            Assert.assertTrue(matcher.matches());
        }
    }
}

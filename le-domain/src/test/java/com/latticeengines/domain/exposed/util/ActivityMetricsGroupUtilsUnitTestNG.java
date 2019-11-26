package com.latticeengines.domain.exposed.util;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;

public class ActivityMetricsGroupUtilsUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsGroupUtilsUnitTestNG.class);

    @Test(groups = "unit", dataProvider = "validAttrNames")
    public void testParseAttrName(String attrName, List<String> expected) {
        try {
            List<String> tokens = ActivityMetricsGroupUtils.parseAttrName(attrName);
            Assert.assertEquals(CollectionUtils.size(tokens), 3);
            Assert.assertEquals(tokens.get(0), expected.get(0));
            Assert.assertEquals(tokens.get(1), expected.get(1));
            Assert.assertEquals(tokens.get(2), expected.get(2));
        } catch (ParseException e) {
            Assert.fail("Should not fail to parse " + attrName, e);
        }
    }

    @DataProvider(name = "validAttrNames")
    public Object[][] provideValidAttrNames() {
        return new Object[][]{
                { "am_twv__pathpatternid__w_2_w", Arrays.asList("twv", "pathpatternid", "w_2_w") },
                { "am_twv__id1_id2__b_2_3_w", Arrays.asList("twv", "id1_id2", "b_2_3_w") },
        };
    }

    @Test(groups = "unit", dataProvider = "invalidAttrNames", expectedExceptions = ParseException.class)
    public void testParseInvalidAttrName(String attrName) throws ParseException {
        ActivityMetricsGroupUtils.parseAttrName(attrName);
    }

    @DataProvider(name = "invalidAttrNames")
    public Object[][] provideInvalidAttrNames() {
        return new Object[][]{ //
                { "foo" }, //
                { "foo bar" }, //
                { "twv__pathpatternid__w_2_w" }, //
                { "am__twv__id1_id2__b_2_3_w" }, //
        };
    }

}

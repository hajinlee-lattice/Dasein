package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class VersionComparisonUtilsTestNG {

    @Test(groups = "unit")
    public void versionCompare() {
        assertEquals(VersionComparisonUtils.compareVersion("1.0.0", "1.0.1"), -1);
        assertEquals(VersionComparisonUtils.compareVersion("1", "1.0.1"), -1);
        assertEquals(VersionComparisonUtils.compareVersion("2", "1.9.9"), 1);
        assertEquals(VersionComparisonUtils.compareVersion("2.0.1", "2.0.1"), 0);
        assertEquals(VersionComparisonUtils.compareVersion("2.0", "2"), 0);
    }

}

package com.latticeengines.security.provider.activedirectory;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class RestActiveDirectoryFilterUnitTestNG {

    @Test(groups = "unit")
    public void isSameDayForDayBefore() {
        long oneDayBefore = System.currentTimeMillis() - 60*60*1000*25;
        assertFalse(new RestActiveDirectoryFilter().isSameDay(oneDayBefore, System.currentTimeMillis()));
    }

    @Test(groups = "unit")
    public void isSameDayForOneHourBefore() {
        System.out.println(System.currentTimeMillis());
        long oneHourBefore = System.currentTimeMillis() - 60*60*1000;
        assertTrue(new RestActiveDirectoryFilter().isSameDay(oneHourBefore, System.currentTimeMillis()));
    }
}

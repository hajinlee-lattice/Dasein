package com.latticeengines.security.provider.activedirectory;

import java.util.Calendar;

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
        long curTime = System.currentTimeMillis();
        System.out.println(curTime);
        long hourBefore = curTime - 60*60*1000;
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(curTime);
        int curHour = c.get(Calendar.HOUR);
        assertTrue((curHour == 0) || (new RestActiveDirectoryFilter().isSameDay(hourBefore, curTime)));
    }
}

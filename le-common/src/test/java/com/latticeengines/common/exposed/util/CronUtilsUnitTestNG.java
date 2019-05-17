package com.latticeengines.common.exposed.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CronUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testPreviousFireTime() {
        // every Wednesday 1 am.
        String cron = "0 0 1 ? * WED";

        DateTime now = DateTime.now();
        DateTime lastWed = CronUtils.getPreviousFireTime(cron);
        DateTime nextWed = CronUtils.getNextFireTime(cron);
        Assert.assertEquals(lastWed.dayOfWeek().get(), DateTimeConstants.WEDNESDAY);
        Assert.assertTrue(lastWed.toDate().compareTo(now.toDate()) <= 0);

        Assert.assertEquals(nextWed.dayOfWeek().get(), DateTimeConstants.WEDNESDAY);
        Assert.assertTrue(nextWed.toDate().compareTo(now.toDate()) >= 0);
        Assert.assertTrue(CronUtils.isValidExpression("0 0 0 31 DEC ? 2099"));
    }

}

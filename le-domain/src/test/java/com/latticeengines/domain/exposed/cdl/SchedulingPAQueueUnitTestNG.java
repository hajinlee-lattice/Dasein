package com.latticeengines.domain.exposed.cdl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class SchedulingPAQueueUnitTestNG {

    // TODO @Joy, enable and make this pass after queue part is fixed, currently t4 will not be in the final set
    @Test(groups = "unit", dataProvider = "fillAllCanRunJobs", enabled = false)
    private void testGetCanRunJobs(SchedulingPAQueue<SchedulingPAObject> queue, List<SchedulingPAObject> input,
            int expectedSizeAfterPush, Set<String> expectedTenants) {
        for (SchedulingPAObject obj : input) {
            queue.add(obj);
        }

        // check size after pushing all input
        Assert.assertEquals(queue.size(), expectedSizeAfterPush, String
                .format("Queue(%s) size after adding %s does not match the expected one", queue.getQueueName(), input));

        Set<String> tenants = queue.fillAllCanRunJobs(new HashSet<>());
        Assert.assertEquals(tenants, expectedTenants, "Resulting can run PA tenants does not match the expected ones");
    }

    @DataProvider(name = "fillAllCanRunJobs")
    private Object[][] provideFillAllCanRunJobsTestCases() {
        return new Object[][] { //
                { //
                        new SchedulingPAQueue<>(newStatus(5, 5, 0), ScheduleNowSchedulingPAObject.class), //
                        Arrays.asList( //
                                newScheduleNowObj("t1", true, false, 1L),
                                // not schedule now, will be skipped during push
                                newScheduleNowObj("t2", false, true, 2L),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newScheduleNowObj("t3", true, true, 3L), //
                                newScheduleNowObj("t4", true, false, 4L)), //
                        3, Sets.newHashSet("t1", "t4"), //
                }, //
                // TODO @Joy add more test cases for different queue/scenario
        }; //
    }

    /*
     * TODO @Joy make these helper more flexible, move to util or builder if needed
     */

    private ScheduleNowSchedulingPAObject newScheduleNowObj(String tenantId, boolean isScheduleNow,
            boolean isLargeTenant, long time) {
        TenantActivity activity = new TenantActivity();
        activity.setTenantId(tenantId);
        activity.setScheduledNow(isScheduleNow);
        activity.setLarge(isLargeTenant);
        activity.setScheduleTime(time);
        return new ScheduleNowSchedulingPAObject(activity);
    }

    private SystemStatus newStatus(int scheduleNowLimit, int totalJobLimit, int largeJobLimit) {
        SystemStatus status = new SystemStatus();
        status.setLargeJobTenantId(new HashSet<>());
        status.setRunningPATenantId(new HashSet<>());
        status.setCanRunScheduleNowJobCount(scheduleNowLimit);
        status.setCanRunJobCount(totalJobLimit);
        status.setCanRunLargeJobCount(largeJobLimit);
        return status;
    }
}

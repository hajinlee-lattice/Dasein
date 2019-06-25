package com.latticeengines.domain.exposed.cdl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.TenantType;

public class SchedulingPAQueueUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAQueueUnitTestNG.class);

    @Test(groups = "unit", dataProvider = "fillAllCanRunJobs")
    private void testGetCanRunJobs(SchedulingPAQueue<SchedulingPAObject> queue, List<SchedulingPAObject> input,
            int expectedSizeAfterPush, Set<String> expectedTenants) {
        for (SchedulingPAObject obj : input) {
            queue.add(obj);
        }
        log.info(JsonUtils.serialize(queue));
        // check size after pushing all input
        Assert.assertEquals(queue.size(), expectedSizeAfterPush, String
                .format("Queue(%s) size after adding %s does not match the expected one", queue.getQueueName(), input));

        Set<String> tenants = queue.fillAllCanRunJobs();
        Assert.assertEquals(tenants, expectedTenants, "Resulting can run PA tenants does not match the expected ones");
    }

    @DataProvider(name = "fillAllCanRunJobs")
    private Object[][] provideFillAllCanRunJobsTestCases() {
        return new Object[][] { //
                { //
                        new SchedulingPAQueue<>(newStatus(5, 6, 1), ScheduleNowSchedulingPAObject.class), //
                        Arrays.asList( //
                                newScheduleNowObj("t1", true, false, 1L, TenantType.QA),
                                // not schedule now, will be skipped during push
                                newScheduleNowObj("t2", false, true, 2L, TenantType.CUSTOMER),
                                newScheduleNowObj("t3", true, true, 3L, TenantType.CUSTOMER), //
                                newScheduleNowObj("t4", true, false, 4L, TenantType.CUSTOMER),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newScheduleNowObj("t5", true, true, 5L, TenantType.CUSTOMER)), //
                        4, Sets.newHashSet("t1", "t3", "t4") //
                }, //
                {
                        new SchedulingPAQueue<>(newStatus(5, 5, 2), RetrySchedulingPAObject.class), //
                        Arrays.asList(
                                newRetryObj("t1", true, false, 1L, TenantType.QA),
                                // not retry, will be skipped during push
                                newRetryObj("t2", false, true, 2L, TenantType.CUSTOMER),
                                newRetryObj("t3", true, true, new Date().getTime(), TenantType.CUSTOMER),
                                newRetryObj("t4", true, true, 3L, TenantType.CUSTOMER),
                                newRetryObj("t5", true, true, 4L, TenantType.CUSTOMER),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newRetryObj("t6", true, true, 5L, TenantType.CUSTOMER)
                        ), 4, Sets.newHashSet("t1", "t4", "t5") //
                },
                {
                        new SchedulingPAQueue<>(newStatus(5, 7, 2), AutoScheduleSchedulingPAObject.class),
                        Arrays.asList(
                                newAutoScheduleObj("t1", true, false, 1L, 1L, 1L, TenantType.QA),
                                // not auto schedule, will be skipped during push
                                newAutoScheduleObj("t2", false, true, 2L, 2L, 2L, TenantType.CUSTOMER),
                                newAutoScheduleObj("t3", true, true, 3L, new Date().getTime() - 6, 3L,
                                        TenantType.CUSTOMER),
                                newAutoScheduleObj("t4", true, true, 4L, 4L, new Date().getTime() - 6,
                                        TenantType.CUSTOMER),
                                newAutoScheduleObj("t5", true, true, 5L, 5L, 5L,
                                        TenantType.CUSTOMER),
                                newAutoScheduleObj("t6", true, true, 6L, 6L, 6L,
                                        TenantType.CUSTOMER),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newAutoScheduleObj("t7", true, true, 7L, 7L, 7L,
                                        TenantType.CUSTOMER)
                        ), 4, Sets.newHashSet("t1", "t5", "t6")
                },
                {
                        new SchedulingPAQueue<>(newStatus(5, 7, 2), DataCloudRefreshSchedulingPAObject.class),
                        Arrays.asList(
                                newDataCloudRefreshObj("t1", true, false, TenantType.QA),
                                // not dataCloud refresh, will be skipped during push
                                newDataCloudRefreshObj("t2", false, true, TenantType.CUSTOMER),
                                newDataCloudRefreshObj("t3", true, true, TenantType.CUSTOMER),
                                newDataCloudRefreshObj("t4", true, true, TenantType.CUSTOMER),
                                newDataCloudRefreshObj("t5", true, false, TenantType.CUSTOMER),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newDataCloudRefreshObj("t6", true, true, TenantType.CUSTOMER)
                        ), 5, Sets.newHashSet("t1", "t3", "t5", "t6")
                },
                { //
                        new SchedulingPAQueue<>(newStatus(2, 6, 1), ScheduleNowSchedulingPAObject.class), //
                        Arrays.asList( //
                                newScheduleNowObj("t1", true, false, 1L, TenantType.QA),
                                // not schedule now, will be skipped during push
                                newScheduleNowObj("t2", false, true, 2L, TenantType.CUSTOMER),
                                newScheduleNowObj("t3", true, true, 3L, TenantType.CUSTOMER), //
                                newScheduleNowObj("t4", true, false, 4L, TenantType.CUSTOMER),
                                // large tenant, will violate large tenant limit since it is set to 0
                                newScheduleNowObj("t5", true, true, 5L, TenantType.CUSTOMER)), //
                        4, Sets.newHashSet("t3", "t4") //
                },
        }; //
    }

    private ScheduleNowSchedulingPAObject newScheduleNowObj(String tenantId, boolean isScheduleNow,
                                                            boolean isLargeTenant, long time, TenantType tenantType) {
        TenantActivity activity = new TenantActivity();
        activity.setTenantId(tenantId);
        activity.setScheduledNow(isScheduleNow);
        activity.setLarge(isLargeTenant);
        activity.setScheduleTime(time);
        activity.setTenantType(tenantType);
        return new ScheduleNowSchedulingPAObject(activity);
    }

    private RetrySchedulingPAObject newRetryObj(String tenantId, boolean isRetry, boolean isLargeTenant,
                                                long lastFinishTime, TenantType tenantType) {
        TenantActivity tenantActivity = new TenantActivity();
        tenantActivity.setTenantId(tenantId);
        tenantActivity.setRetry(isRetry);
        tenantActivity.setLarge(isLargeTenant);
        tenantActivity.setLastFinishTime(lastFinishTime);
        tenantActivity.setTenantType(tenantType);
        return new RetrySchedulingPAObject(tenantActivity);
    }

    private DataCloudRefreshSchedulingPAObject newDataCloudRefreshObj(String tenantId, boolean isDataCloudRefresh,
                                                                      boolean isLargeTenant, TenantType tenantType) {
        TenantActivity tenantActivity = new TenantActivity();
        tenantActivity.setTenantId(tenantId);
        tenantActivity.setDataCloudRefresh(isDataCloudRefresh);
        tenantActivity.setLarge(isLargeTenant);
        tenantActivity.setTenantType(tenantType);
        return new DataCloudRefreshSchedulingPAObject(tenantActivity);
    }

    private AutoScheduleSchedulingPAObject newAutoScheduleObj(String tenantId, boolean isAutoSchedule,
                                                              boolean isLarge, long invokeTime, long firstActionTime,
                                                              long lastActionTime, TenantType tenantType) {
        TenantActivity tenantActivity = new TenantActivity();
        tenantActivity.setTenantId(tenantId);
        tenantActivity.setTenantType(tenantType);
        tenantActivity.setLarge(isLarge);
        tenantActivity.setAutoSchedule(isAutoSchedule);
        tenantActivity.setInvokeTime(invokeTime);
        tenantActivity.setFirstActionTime(firstActionTime);
        tenantActivity.setLastActionTime(lastActionTime);
        return new AutoScheduleSchedulingPAObject(tenantActivity);
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

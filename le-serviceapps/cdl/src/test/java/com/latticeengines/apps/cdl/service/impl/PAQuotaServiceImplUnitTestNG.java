package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_AUTO_SCHEDULE;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_SCHEDULE_NOW;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class PAQuotaServiceImplUnitTestNG {

    private static final LocalDateTime MIDNIGHT = LocalDateTime.of(2020, 12, 25, 0, 0);

    @Test(groups = "unit", dataProvider = "hasQuotaTestData")
    private void testHasQuota(TestData testData, String quotaName, boolean expectedResult) {
        PAQuotaServiceImpl svc = Mockito.spy(new PAQuotaServiceImpl());
        when(svc.getTenantPaQuota(anyString())).thenReturn(testData.quota);

        ZoneId zone = ZoneId.of(testData.timezone);
        Instant now = MIDNIGHT.plus(13L, ChronoUnit.HOURS).atZone(zone).toInstant();
        System.out.println(ZoneId.getAvailableZoneIds());
        List<WorkflowJob> jobs = Arrays.stream(testData.pas).map(pa -> {
            if (pa.isRestart) {
                return restartPA(time(MIDNIGHT.plus(pa.rootPAStartTime, ChronoUnit.HOURS), testData.timezone),
                        time(MIDNIGHT.plus(pa.startTime, ChronoUnit.HOURS), testData.timezone), pa.consumedQuota);
            } else {
                return newPA(time(MIDNIGHT.plus(pa.startTime, ChronoUnit.HOURS), testData.timezone), pa.consumedQuota);
            }
        }).collect(Collectors.toList());

        boolean res = svc.hasQuota("tenant", quotaName, jobs, now, zone);
        Assert.assertEquals(res, expectedResult);

        if (SchedulerConstants.DEFAULT_TIMEZONE.getId().equals(testData.timezone)) {
            res = svc.hasQuota("tenant", quotaName, jobs, now, null);
            Assert.assertEquals(res, expectedResult);
        }
    }

    @DataProvider(name = "hasQuotaTestData")
    private Object[][] hasQuotaTestData() {
        return new Object[][] { //
                /*-
                 * single PA
                 */
                { // no completed PA
                        new TestData("UTC", //
                                new MockPA[0], //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 1L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //
                { // all quota consumed
                        new TestData("Europe/Warsaw", //
                                new MockPA[] { new MockPA(0, false, -1, QUOTA_SCHEDULE_NOW), }, //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 1L) //
                        ), QUOTA_SCHEDULE_NOW, false //
                }, //
                { // 1 PA but consume different quota
                        new TestData("America/Marigot", //
                                new MockPA[] { new MockPA(0, false, -1, QUOTA_AUTO_SCHEDULE), }, //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 1L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //
                { // out of current quota interval
                        new TestData("GMT", //
                                new MockPA[] { new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), }, //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 1L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //
                { // no quota for specific name
                        new TestData("US/Hawaii", //
                                new MockPA[] { new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), }, //
                                ImmutableMap.of(QUOTA_AUTO_SCHEDULE, 1L) //
                        ), QUOTA_SCHEDULE_NOW, false //
                }, //
                { // retried PA in current interval, but original PA is not, therefore still has
                  // quota
                        new TestData("America/Los_Angeles", //
                                new MockPA[] { new MockPA(1, true, -5, QUOTA_SCHEDULE_NOW), }, //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 1L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //

                /*-
                 * multiple PAs
                 */
                { // 3 quota, 2 used in current interval
                        new TestData("America/Los_Angeles", //
                                new MockPA[] { //
                                        new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(1, true, -5, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(3, false, -1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(11, true, 1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(15, false, -1, QUOTA_SCHEDULE_NOW), //
                                }, //
                                ImmutableMap.of(QUOTA_AUTO_SCHEDULE, 1L, QUOTA_SCHEDULE_NOW, 3L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //
                { // 3 quota, 4 used in current interval
                        new TestData("UTC", //
                                new MockPA[] { //
                                        new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(1, true, -5, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(3, false, -1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(11, true, 1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(15, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(19, false, -1, QUOTA_SCHEDULE_NOW), //
                                }, //
                                ImmutableMap.of(QUOTA_AUTO_SCHEDULE, 100L, QUOTA_SCHEDULE_NOW, 3L) //
                        ), QUOTA_SCHEDULE_NOW, false //
                }, //
                { // 1 quota, 0 used in current interval
                        new TestData("UTC", //
                                new MockPA[] { //
                                        new MockPA(-15, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(1, true, -5, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(3, false, -1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(11, true, 1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(12, true, 4, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(16, false, -1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(25, false, -1, QUOTA_SCHEDULE_NOW), //
                                }, //
                                ImmutableMap.of(QUOTA_AUTO_SCHEDULE, 100L, QUOTA_SCHEDULE_NOW, 3L) //
                        ), QUOTA_SCHEDULE_NOW, true //
                }, //
                { // 0 quota, 0 used in current interval
                        new TestData("UTC", //
                                new MockPA[] { //
                                        new MockPA(-15, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(-1, false, -1, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(1, true, -5, QUOTA_SCHEDULE_NOW), //
                                        new MockPA(12, true, 4, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(16, false, -1, QUOTA_AUTO_SCHEDULE), //
                                        new MockPA(25, false, -1, QUOTA_SCHEDULE_NOW), //
                                }, //
                                ImmutableMap.of(QUOTA_SCHEDULE_NOW, 0L) //
                        ), QUOTA_SCHEDULE_NOW, false //
                }, //
        }; //
    }

    private WorkflowJob newPA(Instant startTime, String consumedQuota) {
        WorkflowJob job = new WorkflowJob();
        if (startTime != null) {
            job.setStartTimeInMillis(startTime.toEpochMilli());
        }
        if (StringUtils.isNotBlank(consumedQuota)) {
            setConsumedQuota(job, consumedQuota);
        }
        return job;
    }

    private WorkflowJob restartPA(Instant originalStartTime, Instant startTime, String consumedQuota) {
        WorkflowJob job = newPA(startTime, consumedQuota);
        if (job.getWorkflowConfiguration() == null) {
            job.setWorkflowConfiguration(new WorkflowConfiguration());
        }
        job.getWorkflowConfiguration().setRestart(true);

        setConsumedQuota(job, consumedQuota);
        setRootPAStartTime(job, originalStartTime.toEpochMilli());
        return job;
    }

    private Instant time(LocalDateTime localTime, String timezone) {
        return localTime.atZone(ZoneId.of(timezone)).toInstant();
    }

    private void setConsumedQuota(@NotNull WorkflowJob job, @NotNull String quotaName) {
        addTag(job, WorkflowContextConstants.Tags.CONSUMED_QUOTA_NAME, quotaName);
    }

    private void setRootPAStartTime(@NotNull WorkflowJob job, long timestamp) {
        addTag(job, WorkflowContextConstants.Tags.ROOT_WORKFLOW_START_TIME, String.valueOf(timestamp));
    }

    private void addTag(@NotNull WorkflowJob job, @NotNull String key, @NotNull String value) {
        if (job.getWorkflowConfiguration() == null) {
            job.setWorkflowConfiguration(new WorkflowConfiguration());
        }

        HashMap<String, String> tags = new HashMap<>(MapUtils.emptyIfNull(job.getWorkflowConfiguration().getTags()));
        tags.put(key, value);
        job.getWorkflowConfiguration().setTags(tags);
    }

    private static class TestData {
        String timezone;
        MockPA[] pas;
        Map<String, Long> quota;

        TestData(String timezone, MockPA[] pas, Map<String, Long> quota) {
            this.timezone = timezone;
            this.pas = pas;
            this.quota = quota;
        }
    }

    private static class MockPA {
        int startTime; // hr since midnight
        boolean isRestart; //
        int rootPAStartTime; // hr since midnight
        String consumedQuota;

        MockPA(int startTime, boolean isRestart, int rootPAStartTime, String consumedQuota) {
            this.startTime = startTime;
            this.isRestart = isRestart;
            this.rootPAStartTime = rootPAStartTime;
            this.consumedQuota = consumedQuota;
        }
    }
}

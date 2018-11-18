package com.latticeengines.workflow.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.JobStatus;

public class WorkflowJobUtilsUnitTestNG {

    @Test(groups = { "unit" })
    public void testJobStatusToWorkflowStatus() {
        List<String> workflowStatusForRunning = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Collections.singletonList(JobStatus.RUNNING.name()));
        assertNotNull(workflowStatusForRunning);
        List<String> workflowStatusForRunningLowercase = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Collections.singletonList(JobStatus.RUNNING.name().toLowerCase()));
        assertNotNull(workflowStatusForRunningLowercase);
        assertEquals(workflowStatusForRunningLowercase, workflowStatusForRunning);

        List<String> workflowStatusForCompleted = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Collections.singletonList(JobStatus.COMPLETED.name()));
        assertNotNull(workflowStatusForCompleted);

        List<String> workflowStatusForMultipleValues = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Arrays.asList(JobStatus.RUNNING.name(), JobStatus.COMPLETED.name()));
        assertNotNull(workflowStatusForMultipleValues);
        List<String> combinedList = Stream.of(workflowStatusForRunning, workflowStatusForCompleted)
                .flatMap(Collection::stream).collect(Collectors.toList());

        assertEquals(workflowStatusForMultipleValues.size(), combinedList.size());
        workflowStatusForMultipleValues.forEach(status -> {
            assertTrue(combinedList.contains(status));
        });

        final String invalidType = "Invalid";
        List<String> workflowStatusForInvalidStr = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Collections.singletonList(invalidType));
        assertNotNull(workflowStatusForInvalidStr);
        assertEquals(workflowStatusForInvalidStr.size(), 1);
        assertEquals(workflowStatusForInvalidStr.get(0), invalidType);
    }

    @Test(groups = "unit")
    public void testAdjustDate() {
        String fromZone = "US/Eastern";
        String toZone = "UTC";
        long zoneDiff = zoneDiff(fromZone, toZone);
        ZonedDateTime date1 = ZonedDateTime.of(2018, 10, 22,
                2, 43, 40, 0, ZoneId.of(fromZone));
        Date adjusted = WorkflowJobUtils.adjustDate(Date.from(date1.toInstant()), fromZone, toZone);
        long diff = adjusted.getTime() - Date.from(date1.toInstant()).getTime();
        Assert.assertEquals(TimeUnit.HOURS.toMillis(zoneDiff), diff);

        fromZone = "US/Pacific";
        ZonedDateTime date2 = ZonedDateTime.of(2018, 5, 1,
                4, 12, 13, 0, ZoneId.of(fromZone));
        adjusted = WorkflowJobUtils.adjustDate(Date.from(date2.toInstant()), fromZone, toZone);
        zoneDiff = zoneDiff(fromZone, toZone);
        diff = adjusted.getTime() - Date.from(date2.toInstant()).getTime();
        Assert.assertEquals(TimeUnit.HOURS.toMillis(zoneDiff), diff);
    }

    private long zoneDiff(String fromZone, String toZone) {
        LocalDateTime from = LocalDateTime.now(ZoneId.of(fromZone));
        LocalDateTime to = LocalDateTime.now(ZoneId.of(toZone));
        return ChronoUnit.HOURS.between(from, to);
    }
}

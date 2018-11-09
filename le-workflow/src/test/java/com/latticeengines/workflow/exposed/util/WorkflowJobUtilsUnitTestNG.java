package com.latticeengines.workflow.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
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
        String fromZone = "UTC-05:00";
        String toZone = "UTC";
        ZonedDateTime date1 = ZonedDateTime.of(2018, 10, 22,
                2, 43, 40, 0, ZoneId.of(fromZone));
        ZonedDateTime expectedDate = ZonedDateTime.of(2018, 10, 22,
                7, 43, 40, 0, ZoneId.of(toZone));
        Date adjusted = WorkflowJobUtils.adjustDate(Date.from(date1.toInstant()), fromZone, toZone);
        Assert.assertEquals(adjusted, Date.from(expectedDate.toInstant()));

        fromZone = "UTC+08:00";
        ZonedDateTime date2 = ZonedDateTime.of(2018, 5, 1,
                4, 12, 13, 0, ZoneId.of(fromZone));
        expectedDate = ZonedDateTime.of(2018, 4, 30,
                20, 12, 13, 0, ZoneId.of(toZone));
        adjusted = WorkflowJobUtils.adjustDate(Date.from(date2.toInstant()), fromZone, toZone);
        Assert.assertEquals(adjusted, Date.from(expectedDate.toInstant()));
    }
}

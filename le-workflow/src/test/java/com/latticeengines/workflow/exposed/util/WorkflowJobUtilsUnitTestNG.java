package com.latticeengines.workflow.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.JobStatus;

public class WorkflowJobUtilsUnitTestNG {

    @Test(groups = { "unit" })
    public void testJobStatusToWorkflowStatus() {
        List<String> workflowStatusForRunning = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(Arrays.asList(JobStatus.RUNNING.name()));
        assertNotNull(workflowStatusForRunning);
        List<String> workflowStatusForRunningLowercase = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(Arrays.asList(JobStatus.RUNNING.name().toLowerCase()));
        assertNotNull(workflowStatusForRunningLowercase);
        assertEquals(workflowStatusForRunningLowercase, workflowStatusForRunning);

        List<String> workflowStatusForCompleted = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(Arrays.asList(JobStatus.COMPLETED.name()));
        assertNotNull(workflowStatusForCompleted);

        List<String> workflowStatusForMultipleValues = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(
                Arrays.asList(JobStatus.RUNNING.name(), JobStatus.COMPLETED.name()));
        assertNotNull(workflowStatusForMultipleValues);
        List<String> combinedList = Stream.of(workflowStatusForRunning, workflowStatusForCompleted)
                .flatMap(Collection::stream).collect(Collectors.toList());

        assertEquals(workflowStatusForMultipleValues, combinedList);

        final String invalidType = "Invalid";
        List<String> workflowStatusForInvalidStr = WorkflowJobUtils.getWorkflowJobMappingsForJobStatuses(Arrays.asList(invalidType));
        assertNotNull(workflowStatusForInvalidStr);
        assertEquals(workflowStatusForInvalidStr.size(), 1);
        assertEquals(workflowStatusForInvalidStr.get(0), invalidType);
    }
}

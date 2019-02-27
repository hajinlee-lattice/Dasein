package com.latticeengines.workflow.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;

@ContextConfiguration(locations = {"classpath:test-workflow-context.xml"})
public class RedisJobCacheWriterTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private JobCacheWriter writer;

    private ObjectMapper mapper = new ObjectMapper();

    /*
     * test batch APIs for cache writer
     */
    @Test(groups = "functional", dataProvider = "batchFlowTestObjects", invocationCount = 10)
    public void testBatchFlow(List<Job> jobs, List<Long> extraWorkflowIds, boolean includeDetails) {
        List<Long> totalWorkflowIds = Stream.concat(
                jobs.stream().map(Job::getId),
                extraWorkflowIds.stream()).collect(Collectors.toList());
        clear(totalWorkflowIds, includeDetails);

        // batch get, all cache misses
        List<JobCache> results = writer.getByWorkflowIds(totalWorkflowIds, includeDetails);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), jobs.size() + extraWorkflowIds.size());
        results.forEach(Assert::assertNull);

        // populate cache entries
        writer.put(jobs, includeDetails);

        // batch get entry and update timestamp
        Set<Long> missingJobIds = new HashSet<>(extraWorkflowIds);
        Map<Long, Job> jobMap = jobs.stream().collect(Collectors.toMap(Job::getId, job -> job));
        Collections.shuffle(totalWorkflowIds);
        List<JobCache> caches = writer.getByWorkflowIds(totalWorkflowIds, includeDetails);
        List<Long> updateTimes = writer.getUpdateTimeByWorkflowIds(totalWorkflowIds, includeDetails);
        Assert.assertNotNull(caches);
        Assert.assertEquals(caches.size(), totalWorkflowIds.size());
        Assert.assertEquals(updateTimes.size(), totalWorkflowIds.size());
        for (int i = 0; i < totalWorkflowIds.size(); i++) {
            Long id = totalWorkflowIds.get(i);
            if (missingJobIds.contains(id)) {
                Assert.assertNull(caches.get(i));
                Assert.assertNull(updateTimes.get(i));
            } else {
                JobCache cache = caches.get(i);
                Assert.assertNotNull(cache);
                Assert.assertNotNull(cache.getJob());
                Assert.assertNotNull(cache.getUpdatedAt());
                assertEquals(cache.getJob(), jobMap.get(id));
                Assert.assertEquals(updateTimes.get(i), cache.getUpdatedAt());
            }
        }

        // set/get update time of cach entries
        Long now = System.currentTimeMillis();
        writer.setUpdateTimeByWorkflowIds(extraWorkflowIds, includeDetails, now);
        List<Long> extraUpdateTimes = writer.getUpdateTimeByWorkflowIds(extraWorkflowIds, includeDetails);
        Assert.assertNotNull(extraUpdateTimes);
        Assert.assertEquals(extraUpdateTimes.size(), extraWorkflowIds.size());
        extraUpdateTimes.forEach(time -> Assert.assertEquals(time, now));

        clear(totalWorkflowIds, includeDetails);
    }

    @DataProvider(name = "batchFlowTestObjects")
    private Object[][] provideBatchFlowTestObjects() {
        return new Object[][] {
                {newJobs(123L, 456L, 789L, 1234567L, 987654L), Arrays.asList(111L, 222L, 333L), false},
                {newJobs(123L, 456L, 789L, 1234567L, 987654L), Collections.emptyList(), false},
                {newJobs(123L, 456L, 789L, 1234567L, 987654L), Collections.singletonList(111L), true},
                {newJobs(123L), Arrays.asList(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L), true},
                {Collections.emptyList(), Arrays.asList(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L), true},
        };
    }

    private List<Job> newJobs(long... workflowIds) {
        return Arrays.stream(workflowIds).mapToObj(this::newJob).collect(Collectors.toList());
    }

    /*
     * generate a job with different type of fields
     */
    private Job newJob(long workflowId) {
        Job job = new Job();
        job.setId(workflowId);
        job.setApplicationId("applicationId");
        job.setDescription("description");
        job.setJobStatus(JobStatus.CANCELLED);
        job.setInputs(Collections.singletonMap("key", "value"));
        JobStep step = new JobStep();
        step.setId(123L);
        step.setDescription("step description");
        job.setSteps(Collections.singletonList(step));
        return job;
    }

    private void clear(List<Long> workflowIds, boolean includeDetails) {
        workflowIds.stream().map(id -> {
            Job job = new Job();
            job.setId(id);
            return job;
        }).forEach(job -> writer.clear(job, includeDetails));
    }

    private void assertEquals(Job result, Job expectedJob) {
        try {
            String resultStr = mapper.writeValueAsString(result);
            String expectedStr = mapper.writeValueAsString(expectedJob);
            Assert.assertEquals(resultStr, expectedStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

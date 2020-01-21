package com.latticeengines.workflow.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mockito;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.cache.JobCacheWriter;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@ContextConfiguration(locations = {"classpath:test-workflow-context.xml"})
public class JobCacheServiceTestNG extends AbstractTestNGSpringContextTests {
    private static final Long EXPIRED_TIMESTAMP = 0L;
    private static final Long NEVER_EXPIRE_TIMESTAMP = System.currentTimeMillis() + 7 * 86400 * 1000;  // 1 week
    private static final int MAX_WAIT_ATTEMPTS = 15;
    private static final long WAIT_INTERVALS = 100;
    private static final double MULTIPLIER = 1.5;

    @Inject
    private JobCacheService service;

    @Inject
    private JobCacheWriter writer;

    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private LEJobExecutionRetriever leJobExecutionRetriever;

    private ReportService reportService;

    private RetryTemplate retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
            MAX_WAIT_ATTEMPTS, WAIT_INTERVALS, MULTIPLIER, Collections.singletonMap(Exception.class, true));

    @Value("${common.microservice.url}")
    private String microserviceUrl;

    @BeforeClass(groups = "functional")
    public void setUpShared() throws Exception {
        reportService = Mockito.mock(ReportService.class);
        leJobExecutionRetriever = Mockito.mock(LEJobExecutionRetriever.class);
        configureReportService(reportService);
        Mockito.when(leJobExecutionRetriever.getJobExecution(Mockito.anyLong())).thenReturn(newJobExecution(123L));
        Mockito.when(leJobExecutionRetriever.getJobExecution(Mockito.any(), Mockito.anyBoolean())).thenReturn(newJobExecution(123L));
        FieldUtils.writeField(service, "reportService", reportService, true);
        FieldUtils.writeField(service, "leJobExecutionRetriever", leJobExecutionRetriever, true);
    }

    @BeforeMethod(groups = "functional")
    public void setUp() {
        workflowJobEntityMgr = Mockito.mock(WorkflowJobEntityMgr.class);
    }

    @Test(groups = "functional", dataProvider = "jobCacheTestObj")
    public void getByWorkflowId(JobCacheTest test) throws Exception {
        cleanup(test);
        configureWorkflowEntityManager(Collections.singletonMap(test.workflowId, test.storedJob));
        prepareCacheEntry(test);

        Job result = service.getByWorkflowId(test.workflowId, test.includeDetails);
        verify(result, test);

        cleanup(test);
    }

    @Test(groups = "functional", dataProvider = "jobCacheTestList")
    public void getByWorkflowIds(List<JobCacheTest> tests, boolean includeDetails) throws Exception {
        Map<Long, WorkflowJob> workflowJobMap = tests
                .stream()
                .filter(test -> test.storedJob != null)
                .collect(Collectors.toMap(test -> test.storedJob.getWorkflowId(), test -> test.storedJob));
        tests.forEach(this::cleanup);
        configureWorkflowEntityManager(workflowJobMap);
        tests.forEach(this::prepareCacheEntry);

        List<Long> workflowIds = tests.stream().map(test -> test.workflowId).collect(Collectors.toList());
        List<Job> results = service.getByWorkflowIds(workflowIds, includeDetails);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), workflowJobMap.size());
        Map<Long, Job> resultMap = results.stream().collect(Collectors.toMap(Job::getId, job -> job));
        IntStream.range(0, tests.size()).forEach(i -> verify(resultMap.get(workflowIds.get(i)), tests.get(i)));

        tests.forEach(this::cleanup);
    }

    @DataProvider(name = "jobCacheTestObj")
    private Object[][] provideJobCacheTestObject() {
        return new Object[][]{
                // everything exists
                {newCacheHitTest(123L, JobStatus.RUNNING, true)},
                {newCacheHitTest(123L, JobStatus.COMPLETED, true)},
                // cache expired
                {newOldEntryCacheHitTest(123L, JobStatus.RUNNING, false)},
                {newOldEntryCacheHitTest(123456L, JobStatus.RUNNING, true)},
                // terminal state, cache should not expire
                {newOldEntryCacheHitTest(987654L, JobStatus.COMPLETED, false)},
                {newOldEntryCacheHitTest(987654L, JobStatus.FAILED, false)},
                // cache miss
                {newCacheMissTest(5566L, JobStatus.RUNNING, false)},
                {newCacheMissTest(5566L, JobStatus.COMPLETED, true)},
                // object does not exist
                {newNotExistTest(999999L, JobStatus.RUNNING, true)},
                {newNotExistTest(999999L, JobStatus.FAILED, true)},
        };
    }

    @DataProvider(name = "jobCacheTestList")
    private Object[][] provideJobCacheTestList() {
        return new Object[][]{
                {newTestList(1337L,
                        Arrays.asList(TestType.CACHE_HIT, TestType.CACHE_HIT, TestType.CACHE_HIT, TestType.CACHE_MISS, TestType.CACHE_MISS),
                        Arrays.asList(JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.FAILED, JobStatus.COMPLETED, JobStatus.CANCELLED),
                        true), true},
                {newTestList(5566L,
                        Arrays.asList(TestType.CACHE_MISS, TestType.OLD_ENTRY_CACHE_HIT, TestType.CACHE_HIT, TestType.NOT_EXIST, TestType.NOT_EXIST),
                        Arrays.asList(JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.FAILED, JobStatus.COMPLETED, JobStatus.CANCELLED),
                        false), false},
                {newTestList(3306L,
                        Arrays.asList(TestType.NOT_EXIST, TestType.CACHE_MISS, TestType.NOT_EXIST, TestType.OLD_ENTRY_CACHE_HIT, TestType.OLD_ENTRY_CACHE_HIT),
                        Arrays.asList(JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.FAILED, JobStatus.COMPLETED, JobStatus.CANCELLED),
                        false), false},
                {newTestList(9200L,
                        Arrays.asList(TestType.CACHE_HIT, TestType.OLD_ENTRY_CACHE_HIT, TestType.OLD_ENTRY_CACHE_HIT, TestType.OLD_ENTRY_CACHE_HIT),
                        Arrays.asList(JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.FAILED, JobStatus.COMPLETED),
                        true), true},
                {newTestList(6379L,
                        Arrays.asList(TestType.NOT_EXIST, TestType.NOT_EXIST, TestType.NOT_EXIST),
                        Arrays.asList(JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.FAILED),
                        false), false},
                {newTestList(9527L,
                        Arrays.asList(TestType.OLD_ENTRY_CACHE_HIT, TestType.OLD_ENTRY_CACHE_HIT),
                        Arrays.asList(JobStatus.RUNNING, JobStatus.FAILED),
                        true), true},
                {newTestList(123456789L,
                        Arrays.asList(TestType.CACHE_MISS, TestType.CACHE_MISS),
                        Arrays.asList(JobStatus.COMPLETED, JobStatus.FAILED),
                        true), true},
        };
    }

    private void cleanup(JobCacheTest test) {
        Job job = new Job();
        job.setId(test.workflowId);
        writer.clear(job, true);
        writer.clear(job, false);
        JobCache cache = writer.getByWorkflowId(test.workflowId, test.includeDetails);
        // make sure it is indeed cleared
        Assert.assertNull(cache);
    }

    private void verify(Job result, JobCacheTest test) {
        if (test.storedJob == null) {
            // job does not exist
            Assert.assertNull(result);
        } else {
            if (test.cachedJob == null) {
                // cache miss
                // should match the object stored
                Assert.assertTrue(equals(result, test.storedJob, test.includeDetails));
                // cache should be populated now
                JobCache cache = writer.getByWorkflowId(test.workflowId, test.includeDetails);
                Assert.assertNotNull(cache);
                Assert.assertTrue(equals(cache.getJob(), test.storedJob, test.includeDetails));
            } else if (test.isCacheEntryOld && !test.cachedJob.getJobStatus().isTerminated()) {
                // cache hit, but expired
                // should match the stale cache object
                assertJsonEquals(result, test.cachedJob);
                waitForCacheEntryRefresh(test);
            } else {
                // cache hit, and valid
                // should match the cache object
                assertJsonEquals(result, test.cachedJob);
                JobCache cache = writer.getByWorkflowId(test.workflowId, test.includeDetails);
                Assert.assertNotNull(cache);
                if (test.isCacheEntryOld) {
                    // no async update should happen
                    Assert.assertEquals(cache.getUpdatedAt(), EXPIRED_TIMESTAMP);
                }
            }
        }
    }

    private void waitForCacheEntryRefresh(JobCacheTest test) {
        retryTemplate.execute((ctx) -> {
            JobCache cache = writer.getByWorkflowId(test.workflowId, test.includeDetails);
            if (cache != null && !EXPIRED_TIMESTAMP.equals(cache.getUpdatedAt()) &&
                    equals(cache.getJob(), test.storedJob, test.includeDetails)) {
                // finished
                return null;
            }
            // retry
            throw new RuntimeException("Cache entry is not set yet");
        });
    }

    private void configureWorkflowEntityManager(Map<Long, WorkflowJob> workflowJobMap) throws Exception {
        Mockito.when(workflowJobEntityMgr.findByWorkflowId(Mockito.anyLong())).thenAnswer((invocation) -> {
            long id = invocation.getArgument(0);
            return workflowJobMap.get(id);
        });
        Mockito.when(workflowJobEntityMgr.findByWorkflowIds(Mockito.anyList())).thenAnswer((invocation) -> {
            List<Long> ids = invocation.getArgument(0);
            return ids.stream().map(workflowJobMap::get).filter(Objects::nonNull).collect(Collectors.toList());
        });
        FieldUtils.writeField(service, "workflowJobEntityMgr", workflowJobEntityMgr, true);
    }

    private void prepareCacheEntry(JobCacheTest test) {
        if (test.cachedJob != null) {
            writer.put(test.cachedJob, test.includeDetails);
            // if test needs a old entry, set update time to a small value
            // otherwise set to a large value so that the entry will not be considered expired
            writer.setUpdateTimeByWorkflowIds(
                    Collections.singletonList(test.cachedJob.getId()),
                    test.includeDetails,
                    test.isCacheEntryOld ? EXPIRED_TIMESTAMP : NEVER_EXPIRE_TIMESTAMP);
            // check the entry
            JobCache cache = writer.getByWorkflowId(test.cachedJob.getId(), test.includeDetails);
            Assert.assertNotNull(cache);
            Assert.assertNotNull(cache.getJob());
            Assert.assertNotNull(cache.getUpdatedAt());
        }
    }

    private void configureReportService(ReportService reportService) {
        Report report = new Report();
        report.setPid(123L);
        Mockito.when(reportService.getReportByName(Mockito.anyString())).thenReturn(report);
    }

    private void assertJsonEquals(Object obj1, Object obj2) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Assert.assertEquals(mapper.writeValueAsString(obj1), mapper.writeValueAsString(obj2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean equals(Job result, WorkflowJob expectedJob, boolean includeDetails) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String resultStr = mapper.writeValueAsString(result);
            Job job = WorkflowJobUtils.assembleJob(
                    reportService, leJobExecutionRetriever, microserviceUrl, expectedJob, includeDetails);
            if (result.getOutputs() != null) {
                // remove yarn log link path for easier checks
                result.getOutputs().remove(WorkflowContextConstants.Outputs.YARN_LOG_LINK_PATH);
            }
            String expectedStr = mapper.writeValueAsString(job);
            if (resultStr == null || expectedStr == null) {
                return false;
            }
            return expectedStr.equals(resultStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<JobCacheTest> newTestList(long startId, List<TestType> types, List<JobStatus> statuses, boolean includeDetails) {
        return IntStream
                .range(0, types.size())
                .mapToObj(idx -> {
                    long workflowId = startId + (long) idx;
                    JobStatus status = statuses.get(idx);
                    switch (types.get(idx)) {
                        case CACHE_HIT:
                            return newCacheHitTest(workflowId, status, includeDetails);
                        case OLD_ENTRY_CACHE_HIT:
                            return newOldEntryCacheHitTest(workflowId, status, includeDetails);
                        case CACHE_MISS:
                            return newCacheMissTest(workflowId, status, includeDetails);
                        default:
                            return newNotExistTest(workflowId, status, includeDetails);
                    }
                }).collect(Collectors.toList());
    }

    /*
     * Job doesn't exist in database
     */
    private JobCacheTest newNotExistTest(long workflowId, JobStatus status, boolean includeDetails) {
        return newTest(workflowId, status, false, false, includeDetails, false);
    }

    /*
     * Job exists in both cache and database
     */
    private JobCacheTest newCacheHitTest(long workflowId, JobStatus status, boolean includeDetails) {
        return newTest(workflowId, status, true, true, includeDetails, false);
    }

    /*
     * Job exists in both cache and database but the cache entry is old (updated a certain amount of time ago)
     */
    private JobCacheTest newOldEntryCacheHitTest(long workflowId, JobStatus status, boolean includeDetails) {
        return newTest(workflowId, status, true, true, includeDetails, true);
    }

    /*
     * Job exists in database but not cache
     */
    private JobCacheTest newCacheMissTest(long workflowId, JobStatus status, boolean includeDetails) {
        return newTest(workflowId, status, false, true, includeDetails, false);
    }

    private JobCacheTest newTest(long workflowId, JobStatus status, boolean cacheEntryExists, boolean objectExists,
                                 boolean includeDetails, boolean isCacheEntryOld) {
        WorkflowJob workflowJob = objectExists ? newWorkflowJob(workflowId, status) : null;
        Job job = cacheEntryExists ? newJob(workflowId, status) : null;
        return new JobCacheTest(workflowId, job, workflowJob, includeDetails, isCacheEntryOld);
    }

    private Job newJob(long workflowid, JobStatus status) {
        Job job = new Job();
        job.setId(workflowid);
        job.setJobStatus(status);
        // dummy field to keep the cache entry valid
        job.setEndTimestamp(new Date());
        job.setTenantId("tenant");
        job.setTenantPid(123L);
        return job;
    }

    private JobExecution newJobExecution(long executionId) {
        JobExecution execution = new JobExecution(executionId);
        execution.setCreateTime(new Date());
        execution.setEndTime(new Date());
        JobInstance instance = new JobInstance(123L, "dummy_job_instance");
        execution.setJobInstance(instance);
        return execution;
    }

    private WorkflowJob newWorkflowJob(long workflowId, JobStatus status) {
        WorkflowJob job = new WorkflowJob();
        job.setWorkflowId(workflowId);
        job.setPid(123L);
        job.setApplicationId("appId");
        job.setParentJobId(789L);
        job.setInputContext(Collections.singletonMap("inputContextKey", "inputContextValue"));
        job.setOutputContext(Collections.singletonMap("outputContextKey", "outputContextValue"));
        job.setStatus(status.getName());
        job.setReportContext(Collections.emptyMap());
        return job;
    }

    /*
     * Test parameter holder
     */
    private static class JobCacheTest {
        final Long workflowId;
        final Job cachedJob;
        final WorkflowJob storedJob;
        final boolean includeDetails;
        final boolean isCacheEntryOld;

        JobCacheTest(Long workflowId, Job cachedJob, WorkflowJob storedJob, boolean includeDetails, boolean isCacheEntryOld) {
            this.workflowId = workflowId;
            this.cachedJob = cachedJob;
            this.storedJob = storedJob;
            this.includeDetails = includeDetails;
            this.isCacheEntryOld = isCacheEntryOld;
        }
    }

    private enum TestType {
        CACHE_HIT, OLD_ENTRY_CACHE_HIT, CACHE_MISS, NOT_EXIST
    }
}

package com.latticeengines.workflow.cache;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobListCache;

@ContextConfiguration(locations = {"classpath:test-workflow-context.xml"})
public class RedisTenantJobIdListCacheWriterTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private TenantJobIdListCacheWriter writer;

    private ObjectMapper mapper = new ObjectMapper();

    @Test(groups = "functional", dataProvider = "jobIdListCacheProvider")
    public void testBasicFlow(Tenant tenant, List<Job> jobs) throws Exception {
        writer.clear(tenant);

        JobListCache listCache = writer.get(tenant);
        Assert.assertNull(listCache);

        writer.set(tenant, jobs);

        listCache = writer.get(tenant);
        Assert.assertNotNull(listCache);
        Assert.assertNotNull(listCache.getUpdatedAt());
        Assert.assertNotNull(listCache.getJobs());
        Assert.assertEquals(listCache.getJobs().size(), jobs.size());
        for (int i = 0; i < jobs.size(); i++) {
            Job job = listCache.getJobs().get(i);
            Job expectedJob = jobs.get(i);
            Assert.assertEquals(mapper.writeValueAsString(job), mapper.writeValueAsString(expectedJob));
        }

        writer.clear(tenant);

        listCache = writer.get(tenant);
        Assert.assertNull(listCache);
    }

    @DataProvider(name = "jobIdListCacheProvider")
    private Object[][] provider() {
        return new Object[][] {
                { newTenant(123L), getTestList() }
        };
    }

    private List<Job> getTestList() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(newJob(123L, 456L, "appId"));
        jobs.add(newJob(null, null, "appId"));
        jobs.add(newJob(123L, 456L, null));
        jobs.add(newJob(123L, null, "appId"));
        jobs.add(newJob(null, null, "appId"));
        return jobs;
    }

    private Job newJob(Long id, Long pid, String applicationId) {
        Job job = new Job();
        job.setId(id);
        job.setPid(pid);
        job.setApplicationId(applicationId);
        return job;
    }

    private Tenant newTenant(long pid) {
        Tenant tenant = new Tenant();
        tenant.setPid(pid);
        return tenant;
    }
}

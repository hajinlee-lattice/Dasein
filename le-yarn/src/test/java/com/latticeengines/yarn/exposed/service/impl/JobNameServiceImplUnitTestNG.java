package com.latticeengines.yarn.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import com.latticeengines.yarn.exposed.service.impl.JobNameServiceImpl;

public class JobNameServiceImplUnitTestNG {

    private JobNameServiceImpl jobNameService = new JobNameServiceImpl();

    @Test(groups = "unit")
    public void testDelimitedJobName() throws Exception {
        final String customer = "Dell";
        final String jobType = "python";

        String jobName = jobNameService.createJobName(customer, jobType);

        assertEquals(customer, jobNameService.getCustomerFromJobName(jobName));

        assertNotEquals(customer + jobType, jobName);
    }
}

package com.latticeengines.yarn.exposed.bean;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.junit.Assert;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.yarn.client.CommandYarnClient;
import org.testng.annotations.Test;

import com.latticeengines.yarn.exposed.service.EMREnvService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-yarn-context.xml" })
public class SpringYarnClientFactoryTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private EMREnvService emrEnvService;

    @Test(groups = "manual")
    public void testYarnClient() {
        String emrCluster = "devcluster_20181019";
        CommandYarnClient yarnClient = emrEnvService.getSpringYarnClient(emrCluster);
        List<ApplicationReport> apps = yarnClient.listApplications();
        Assert.assertTrue(CollectionUtils.isNotEmpty(apps));
    }

}

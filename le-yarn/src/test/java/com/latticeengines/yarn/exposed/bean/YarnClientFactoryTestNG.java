package com.latticeengines.yarn.exposed.bean;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Assert;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.yarn.exposed.service.EMREnvService;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-yarn-context.xml" })
public class YarnClientFactoryTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private EMREnvService emrEnvService;

    @Test(groups = "manual")
    public void testYarnClient() throws IOException, YarnException {
        String emrCluster = "qa_b";
        try (YarnClient yarnClient = emrEnvService.getYarnClient(emrCluster)) {
            yarnClient.start();
            List<ApplicationReport> apps = yarnClient.getApplications();
            Assert.assertTrue(CollectionUtils.isNotEmpty(apps));
        }
        ClusterMetrics clusterMetrics = emrEnvService.getClusterMetrics(emrCluster);
        Assert.assertNotNull(clusterMetrics);
        Assert.assertTrue(clusterMetrics.availableMB > 0);
    }

}

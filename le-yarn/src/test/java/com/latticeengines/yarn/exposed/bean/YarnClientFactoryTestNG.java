package com.latticeengines.yarn.exposed.bean;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Assert;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
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
            List<ApplicationReport> apps = yarnClient.getApplications(Sets.newEnumSet(Arrays.asList(//
                    YarnApplicationState.NEW, //
                    YarnApplicationState.NEW_SAVING, //
                    YarnApplicationState.SUBMITTED, //
                    YarnApplicationState.ACCEPTED, //
                    YarnApplicationState.RUNNING //
            ), YarnApplicationState.class));
            Assert.assertTrue(CollectionUtils.isNotEmpty(apps));
            for (ApplicationReport app : apps) {
                ApplicationResourceUsageReport usageReport = app
                        .getApplicationResourceUsageReport();
                Resource used = usageReport.getUsedResources();
                // no resource usage after SLOW_START_THRESHOLD
                // must be stuck
                Resource asked = usageReport.getNeededResources();
                long mb = asked.getMemorySize();
                int vcores = asked.getVirtualCores();
                System.out.println("appId=" + app.getApplicationId() + " status=" + app.getYarnApplicationState() + " mb=" + mb + " vcores=" + vcores);
            }
        }
        ClusterMetrics clusterMetrics = emrEnvService.getClusterMetrics(emrCluster);
        System.out.println(JsonUtils.serialize(clusterMetrics));
        Assert.assertNotNull(clusterMetrics);
        Assert.assertTrue(clusterMetrics.availableMB > 0);
    }

}

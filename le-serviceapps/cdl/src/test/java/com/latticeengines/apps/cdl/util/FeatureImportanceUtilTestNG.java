package com.latticeengines.apps.cdl.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.util.FeatureImportanceUtil;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.service.impl.ContextResetTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class, ContextResetTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class FeatureImportanceUtilTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private FeatureImportanceUtil featureImportanceUtil;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Inject
    private Configuration yarnConfiguration;

    private String tenantId;
    private ModelSummary modelSummary;
    private String hdfsFilePath;

    @BeforeClass
    private void setup() throws IOException {
        Resource rfModelFile = new ClassPathResource("util/rf_model.txt",
                Thread.currentThread().getContextClassLoader());
        String hdfsFilePathPattern = "{0}/{1}/models/{2}/{3}/{4}/rf_model.txt";
        tenantId = CustomerSpace.parse(TestFrameworkUtils.generateTenantName()).toString();
        String eventTableId = "SomeEventTableIdForFeatureImportanceUtilFunctionalTestNG";
        String uuid = UUID.randomUUID().toString();
        String applicationIdTimeStamp = String.valueOf(System.currentTimeMillis()) + "_010";
        hdfsFilePath = MessageFormat.format(hdfsFilePathPattern, //
                modelingServiceHdfsBaseDir, // 0;should be
                                            // /user/s-analytics/customers/
                tenantId, // 1
                eventTableId, // 2
                uuid, // 3
                applicationIdTimeStamp);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, rfModelFile.getInputStream(), hdfsFilePath);

        modelSummary = new ModelSummary();
        modelSummary.setId("ms_" + UUID.randomUUID().toString());
        modelSummary.setLookupId(tenantId + "|" + eventTableId + "|" + uuid);
        modelSummary.setApplicationId("application_" + applicationIdTimeStamp);
    }

    @Test(groups = "unit")
    private void testFeatureImportanceCompilation() {
        Map<String, Integer> map = featureImportanceUtil.getFeatureImportance(tenantId, modelSummary);
        Assert.assertNotNull(map);
        Assert.assertTrue(map.size() > 0);

        Assert.assertFalse(map.containsKey("LE_REVENUE_RANGE_110M"));
        Assert.assertTrue(map.containsKey("LE_REVENUE"));
        Assert.assertTrue(map.get("LE_REVENUE") == 23);

        Assert.assertFalse(map.containsKey("State_PA"));
        Assert.assertTrue(map.containsKey("State"));
        Assert.assertTrue(map.get("State") == 28);

        Assert.assertFalse(map.containsKey("MarketingAutomationTopAttributes___ISNULL__"));
        Assert.assertTrue(map.containsKey("MarketingAutomationTopAttributes"));
        Assert.assertTrue(map.get("MarketingAutomationTopAttributes") == 20);

        Assert.assertFalse(map.containsKey("BusinessTechnologiesRecentlyDetectedGrouped"));
        Assert.assertTrue(map.containsKey("BusinessTechnologiesRecentlyDetected"));
        Assert.assertTrue(map.get("BusinessTechnologiesRecentlyDetected") == 8);

    }

    @AfterTest
    private void cleanUp() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + tenantId);
    }
}

package com.latticeengines.pls.workflow;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.locks.RateLimitedResourceManager;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.BulkScoringRateLimitingService;
import com.latticeengines.pls.service.impl.BulkScoringRateLimitingServiceImpl;

public class BulkScoringRateLimitDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Inject
    private BulkScoringRateLimitingService rateLimitingService;

    private String folder = "/tmp/BulkScoringRateLimitDeploymentTestNG";
    private String file1 = folder + "/file1.csv";
    private String file2 = folder + "/file2.csv";
    private String file3 = folder + "/file3.csv";

    private String internalUser = "user1@lattice-engines.com";

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        super.setup();
        if (!HdfsUtils.fileExists(yarnConfiguration, folder)) {
            HdfsUtils.mkdir(yarnConfiguration, folder);
        }
    }

    @AfterClass(groups = "deployment.lp")
    public void afterClass() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, folder);
    }

    @AfterMethod(groups = "deployment.lp")
    public void afterMethod() {
        ReflectionTestUtils.setField(rateLimitingService, "bulkRequestRegulatorRegistered", false);
        RateLimitedResourceManager.deregisterResource(
                BulkScoringRateLimitingServiceImpl.BULK_SCORING_REQUEST_REGULATOR + "_" + mainTestTenant.getName());
    }

    @Test(groups = "deployment.lp", dataProvider = "dataProvider")
    public void checkBulkScoringRateLimit(int fileQuata, int rowQuata, int fileRows1, int fileRows2, int fileRows3,
            boolean success) throws Exception {
        writeFile(file1, "a", fileRows1);
        writeFile(file2, "b", fileRows2);
        writeFile(file3, "c", fileRows3);
        ReflectionTestUtils.setField(rateLimitingService, "bulkRequestsPerHour", fileQuata);
        ReflectionTestUtils.setField(rateLimitingService, "bulkRowsPerHour", rowQuata);

        ImportAndRTSBulkScoreWorkflowSubmitter submitter = new ImportAndRTSBulkScoreWorkflowSubmitter();
        ReflectionTestUtils.setField(submitter, "yarnConfiguration", yarnConfiguration);
        ReflectionTestUtils.setField(submitter, "rateLimitingService", rateLimitingService);

        submitter.checkBulkScoringRateLimit(file1, mainTestTenant.getName(), internalUser);
        submitter.checkBulkScoringRateLimit(file2, mainTestTenant.getName(), internalUser);
        LedpException e = null;
        try {
            submitter.checkBulkScoringRateLimit(file3, mainTestTenant.getName(), internalUser);
        } catch (LedpException ex) {
            e = ex;
        }

        if (success) {
            Assert.assertNull(e, "Ratelimiting should not fail.");
        } else {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_18214);
        }

    }

    @DataProvider(name = "dataProvider")
    public Object[][] getDataProvider() {
        return new Object[][] { //
                { 3, 30, 10, 10, 2, true }, //
                { 3, 30, 10, 10, 10, true }, //
                { 3, 30, 10, 10, 11, false }, //
                { 4, 30, 10, 10, 11, false }, //
                { 2, 30, 10, 10, 2, false }, //
                { 2, 30, 10, 10, 10, false }, //
                { 4, 40, 10, 10, 10, true }, //
        };
    }

    private void writeFile(String filePath, String prefix, int count) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(prefix + i + "first,last\n");
        }
        HdfsUtils.writeToFile(yarnConfiguration, filePath, builder.toString());
    }

}

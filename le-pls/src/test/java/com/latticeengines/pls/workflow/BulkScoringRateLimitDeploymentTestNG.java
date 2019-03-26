package com.latticeengines.pls.workflow;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class BulkScoringRateLimitDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private SourceFileService sourceFileService;

    private String file1 = "file1.csv";
    private String file2 = "file2.csv";
    private String file3 = "file3.csv";

    private SourceFile sourceFile1;
    private SourceFile sourceFile2;
    private SourceFile sourceFile3;
    private String internalUser = "user1@lattice-engines.com";
    private String externalUser = "user1@dummy.com";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        sourceFileService.delete(sourceFile1);
        sourceFileService.delete(sourceFile2);
        sourceFileService.delete(sourceFile3);
    }

    @Test(groups = "functional", dataProvider = "dataProvider")
    public void checkBulkScoringRateLimi(int rowQuata, int fileRows1, int fileRows2, int fileRows3, String email,
            boolean success) throws Exception {
        sourceFile1 = createSourceFile("application_1553048841184_0001", file1, mainTestTenant, fileRows1);
        sourceFile2 = createSourceFile("application_1553048841184_0002", file2, mainTestTenant, fileRows2);
        sourceFile3 = createSourceFile("application_1553048841184_0003", file3, mainTestTenant, fileRows3);

        ImportAndRTSBulkScoreWorkflowSubmitter submitter = new ImportAndRTSBulkScoreWorkflowSubmitter();

        WorkflowProxy workflowProxy = mock(WorkflowProxy.class);
        Job job1 = new Job();
        job1.setApplicationId("application_1553048841184_0001");
        Job job2 = new Job();
        job2.setApplicationId("application_1553048841184_0002");
        Job job3 = new Job();
        job3.setApplicationId("application_1553048841184_0003");
        when(workflowProxy.getJobs(null, Arrays.asList("importAndRTSBulkScoreWorkflow"),
                Arrays.asList(JobStatus.RUNNING.getName(), JobStatus.PENDING.getName(), JobStatus.READY.getName()),
                false, mainTestTenant.getId())).thenReturn(Arrays.asList(job1, job2, job3));
        ReflectionTestUtils.setField(submitter, "workflowProxy", workflowProxy);
        ReflectionTestUtils.setField(submitter, "sourceFileService", sourceFileService);
        ReflectionTestUtils.setField(submitter, "bulkRowsQuota", rowQuata);

        LedpException e = null;
        try {
            submitter.checkBulkScoringRateLimit(mainTestTenant.getId(), email);
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
                { 30, 10, 10, 2, internalUser, true }, //
                { 30, 10, 10, 10, internalUser, true }, //
                { 30, 10, 10, 11, internalUser, false }, //
                { 30, 10, 10, 11, externalUser, true }, //
                { 40, 10, 10, 11, internalUser, true }, //
        };
    }

    private SourceFile createSourceFile(String appId, String fileName, Tenant tenant, long count) throws IOException {
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName(fileName);
        sourceFile.setPath(fileName);
        sourceFile.setTenant(tenant);
        sourceFile.setTenantId(tenant.getPid());
        sourceFile.setFileRows(count);
        sourceFile.setApplicationId(appId);
        sourceFileService.create(sourceFile);
        return sourceFile;
    }

}

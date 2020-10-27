package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.CDLWorkflowFunctionalTestNGBase;
import com.latticeengines.cdl.workflow.steps.validations.service.impl.AccountFileValidationService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;

public class AccountFileValidationServiceFunctionalTestNG extends CDLWorkflowFunctionalTestNGBase {


    @Inject
    private AccountFileValidationService accountFileValidationService;

    private static final String ACCOUNT_FILE_DESTINATION = "tmp/validation/account/";

    private String fileName;

    @Override
    protected String getFlowBeanName() {
        return null;
    }

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        InputStream in = testArtifactService.readTestArtifactAsStream(TEST_AVRO_DIR, TEST_AVRO_VERSION, "Account1.avro");
        HdfsUtils.rmdir(yarnConfiguration, ACCOUNT_FILE_DESTINATION);
        fileName = "account.avro";
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, in, ACCOUNT_FILE_DESTINATION + fileName);
    }

    @AfterClass(groups = {"functional"})
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, ACCOUNT_FILE_DESTINATION);
    }

    @Test(groups = "functional")
    public void testAccountValidations() {
        AccountFileValidationConfiguration configuration = new AccountFileValidationConfiguration();
        configuration.setEntity(BusinessEntity.Account);
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(false);
        configuration.setPathList(Collections.singletonList(ACCOUNT_FILE_DESTINATION + fileName));

        List<String> processedRecords = Collections.singletonList("1000");
        EntityValidationSummary summary = accountFileValidationService.validate(configuration, processedRecords);
        Assert.assertNotNull(summary);
        Assert.assertEquals(summary.getErrorLineNumber(), 0L);

        // entity match GA
        processedRecords = Arrays.asList("1000");
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(true);
        summary = accountFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(1000L, summary.getErrorLineNumber());
        Assert.assertEquals("0", processedRecords.get(0));

        // entity match
        processedRecords = Collections.singletonList("50");
        configuration.setEnableEntityMatch(true);
        configuration.setEnableEntityMatchGA(false);
        summary = accountFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(0L, summary.getErrorLineNumber());
    }

}

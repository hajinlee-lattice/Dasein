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
import com.latticeengines.cdl.workflow.steps.validations.service.impl.ContactFileValidationService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;

public class ContactFileValidationServiceFunctionalTestNG extends CDLWorkflowFunctionalTestNGBase {

    @Inject
    private ContactFileValidationService contactFileValidationService;

    private static final String CONTACT_FILE_DESTINATION = "tmp/validation/contact/";


    private String fileName;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        InputStream in = testArtifactService.readTestArtifactAsStream(TEST_AVRO_DIR, TEST_AVRO_VERSION, "Cccount1" +
                ".avro");
        HdfsUtils.rmdir(yarnConfiguration, CONTACT_FILE_DESTINATION);
        fileName = "contact.avro";
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, in, CONTACT_FILE_DESTINATION + fileName);
    }

    @AfterClass(groups = {"functional"})
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, CONTACT_FILE_DESTINATION);
    }
    @Override
    protected String getFlowBeanName() {
        return null;
    }

    @Test(groups = "functional")
    public void testContactFileValidations() {
        // legacy tenant
        ContactFileValidationConfiguration configuration = new ContactFileValidationConfiguration();
        configuration.setEntity(BusinessEntity.Contact);
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(false);
        configuration.setPathList(Collections.singletonList(CONTACT_FILE_DESTINATION + fileName));

        List<String> processedRecords = Collections.singletonList("50");
        EntityValidationSummary summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertNotNull(summary);
        Assert.assertEquals(summary.getErrorLineNumber(), 0L);

        // entity match GA
        processedRecords = Arrays.asList("50");
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(true);
        summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(50L, summary.getErrorLineNumber());
        Assert.assertEquals(processedRecords.get(0), "0");

        // entity match
        processedRecords = Collections.singletonList("50");
        configuration.setEnableEntityMatch(true);
        configuration.setEnableEntityMatchGA(false);
        summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(0L, summary.getErrorLineNumber());

    }
}
